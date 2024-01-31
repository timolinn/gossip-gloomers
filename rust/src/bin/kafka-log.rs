use std::{
    collections::{HashMap, LinkedList},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use anyhow::{bail, Context};
use async_trait::async_trait;
use nazgul::{main_loop, Body, Message, Node, KV};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct KafkaLog {
    id: AtomicUsize,
    logs: HashMap<String, LinkedList<Log>>,
    commit_offsets: HashMap<String, usize>,
    node: String,
    lin_store: String,
    seq_store: String,
    rpc: Mutex<HashMap<usize, oneshot::Sender<Message<Payload>>>>,
    output: Mutex<std::io::Stdout>,
}

#[async_trait]
impl nazgul::KV<usize> for KafkaLog {
    async fn sync_cas(
        &self,
        key: String,
        store: &String,
        from: usize,
        to: usize,
        put: bool,
    ) -> anyhow::Result<()> {
        let msg = Message::new(
            self.node.clone(),
            store.to_string(),
            Body {
                id: self.id.fetch_add(1, Ordering::SeqCst).into(),
                in_reply_to: None,
                payload: Payload::Cas {
                    key: key.into(),
                    from,
                    to,
                    put,
                },
            },
        );

        let res = self.sync_rpc(msg).await.context("sending rpc for cas")?;
        match res.body.payload {
            Payload::CasOk {} => Ok(()),
            _ => anyhow::bail!("unexpected payload for CAS"),
        }
    }

    async fn sync_read(&self, key: String, store: &String) -> anyhow::Result<usize> {
        eprintln!("sync_read|> key");
        let msg = Message::new(
            self.node.clone(),
            store.to_string(),
            Body {
                id: self.id.fetch_add(1, Ordering::SeqCst).into(),
                in_reply_to: None,
                payload: Payload::Read { key: key.into() },
            },
        );
        let res = self.sync_rpc(msg).await.context("sending rpc")?;
        eprintln!("sync_read|> done");
        match res.body.payload {
            Payload::ReadOk { value } => Ok(value),
            _ => bail!("unexpected return type"),
        }
    }

    async fn sync_write(&self, key: String, store: &String, val: usize) -> anyhow::Result<()> {
        let msg = Message::new(
            self.node.clone(),
            store.to_string(),
            Body {
                id: self.id.fetch_add(1, Ordering::SeqCst).into(),
                in_reply_to: None,
                payload: Payload::Write {
                    key: key.into(),
                    value: val,
                },
            },
        );

        let res = self.sync_rpc(msg).await.context("sending rpc for write")?;
        match res.body.payload {
            Payload::WriteOk {} => Ok(()),
            _ => bail!("unexpected payload for write RPC"),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct Log {
    value: usize,
}

impl Log {
    pub fn from(value: usize) -> Self {
        Self { value }
    }
}

impl KafkaLog {
    async fn sync_rpc(&self, msg: Message<Payload>) -> anyhow::Result<Message<Payload>> {
        let (tx, rx) = oneshot::channel::<Message<Payload>>();

        // register transmitter
        self.rpc.lock().unwrap().insert(msg.body.id.unwrap(), tx);
        msg.send(&self.output).context("sending rpc")?;
        eprintln!("sync_rpc|> msg sent");
        Ok(rx.recv()?)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<[usize; 2]>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },

    // store
    Read {
        key: String,
    },
    ReadOk {
        value: usize,
    },
    Write {
        key: String,
        value: usize,
    },
    WriteOk,
    Cas {
        key: String,
        from: usize,
        to: usize,
        put: bool,
    },
    CasOk,
}

#[async_trait]
impl Node<(), Payload> for KafkaLog {
    async fn from_init(
        _state: (),
        init: nazgul::Init,
        _tx: std::sync::mpsc::Sender<nazgul::Message<Payload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: AtomicUsize::new(1),
            logs: HashMap::new(),
            commit_offsets: HashMap::new(),
            node: init.node_id,
            lin_store: "lin-kv".to_string(),
            seq_store: "seq-kv".to_string(),
            rpc: Mutex::new(HashMap::new()),
            output: Mutex::new(std::io::stdout()),
        })
    }

    async fn step(&mut self, input: nazgul::Message<Payload>) -> anyhow::Result<()> {
        eprintln!("KafkaLog|> {:?}", input);
        if self
            .rpc
            .lock()
            .unwrap()
            .contains_key(&input.body.id.unwrap())
        {
            let tx = self
                .rpc
                .lock()
                .unwrap()
                .remove(&input.body.id.unwrap())
                .unwrap();
            if let Err(e) = tx.send(input).context("sending rpc") {
                bail!("channel closed: {}", e.to_string());
            }
            return Ok(());
        }

        eprintln!("KafkaLog2|> {:?}", input);
        let i = input.clone();
        let mut reply = input.into_reply(Some(self.id.get_mut()));
        match reply.body.payload {
            Payload::Send { key, msg } => {
                // let res = self.logs.entry(key.clone()).or_default();
                // res.push_back(Log::from(msg));

                let latest_key = format!("{}:latest", key);
                let offset = self
                    .sync_read(latest_key.clone(), &self.lin_store)
                    .await
                    .context("read offset");
                let mut offset = match offset {
                    Ok(o) => o,
                    Err(_) => 0,
                };
                eprintln!("KafkaLog3|> {:?}", i);
                loop {
                    let curr = offset;
                    let (prev, now) = (curr - 1, curr);
                    let res = self
                        .sync_cas(latest_key.clone(), &self.lin_store, prev, now, true)
                        .await
                        .context("cas offset");

                    match res {
                        Ok(_) => break,
                        Err(_) => offset += 1,
                    };
                }
                eprintln!("KafkaLog4|> {:?}", i);
                let msg_key = format!("{}:{}", key, msg);

                self.sync_write(msg_key, &self.seq_store, msg)
                    .await
                    .context("write msg_key offset")?;

                self.sync_write(latest_key, &self.seq_store, msg)
                    .await
                    .context("write latest key with offset")?;

                reply.body.payload = Payload::SendOk { offset };
                reply.send(&self.output).context("reply Send")?;
            }
            Payload::Poll { offsets } => {
                let o = offsets.clone();
                let mut resp: HashMap<String, Vec<[usize; 2]>> = HashMap::new();

                for (k, v) in offsets {
                    for i in v..v + 3 {
                        let val = self
                            .sync_read(format!("{}:{}", k, i), &self.seq_store)
                            .await
                            .context("read msg_key offset");
                        let val = match val {
                            Ok(o) => o,
                            Err(_) => continue,
                        };
                        let l = resp.entry(k.clone()).or_default();
                        l.push([i, val]);
                    }
                }

                eprintln!("Poll|> {:?}, RESP|>{:?}", o, resp);
                reply.body.payload = Payload::PollOk { msgs: resp };
                reply.send(&self.output).context("reply Poll")?;
            }
            Payload::CommitOffsets { offsets } => {
                for (k, v) in offsets {
                    self.sync_write(format!("commit:{}", k), &self.seq_store, v)
                        .await
                        .context("write offset")?;
                }

                reply.body.payload = Payload::CommitOffsetsOk;
                reply.send(&self.output).context("reply CommitOffsets")?;
            }
            Payload::ListCommittedOffsets { keys } => {
                let mut resp = HashMap::new();
                for k in keys {
                    let val = self
                        .sync_read(format!("commit:{}", k), &self.seq_store)
                        .await
                        .context("read msg_key offset");
                    let val = match val {
                        Ok(o) => o,
                        Err(_) => continue,
                    };
                    resp.insert(k, val);
                }
                reply.body.payload = Payload::ListCommittedOffsetsOk { offsets: resp };
                reply
                    .send(&self.output)
                    .context("reply ListCommittedOffsets")?;
            }
            Payload::PollOk { .. }
            | Payload::SendOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. } => {}
            Payload::Read { key: _ } => todo!(),
            Payload::ReadOk { value: _ } => todo!(),
            Payload::Write { key: _, value: _ } => todo!(),
            Payload::WriteOk => todo!(),
            Payload::Cas {
                key: _,
                from: _,
                to: _,
                put: _,
            } => todo!(),
            Payload::CasOk => todo!(),
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaLog, _>(())
}
