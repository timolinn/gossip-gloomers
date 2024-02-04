#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use std::{
    collections::{HashMap, LinkedList},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use anyhow::{bail, Context};
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

impl nazgul::KV<usize> for KafkaLog {
    fn sync_cas(
        &self,
        key: impl Into<String>,
        store: &String,
        from: usize,
        to: usize,
        put: bool,
    ) -> anyhow::Result<()> {
        let k: String = key.into();
        eprintln!("sync_cas|> {}", k);
        let msg = Message::new(
            self.node.clone(),
            store.to_string(),
            Body {
                id: self.id.fetch_add(1, Ordering::SeqCst).into(),
                in_reply_to: None,
                payload: Payload::Cas {
                    key: k,
                    from,
                    to,
                    put,
                },
            },
        );

        let res = self.sync_rpc(msg).context("sending rpc for cas")?;
        match res.body.payload {
            Payload::CasOk {} => Ok(()),
            _ => anyhow::bail!("unexpected payload for CAS"),
        }
    }

    fn sync_read(&self, key: impl Into<String>, store: &String) -> anyhow::Result<usize> {
        let k: String = key.into();
        eprintln!("sync_read|> {}", k);
        let msg = Message::new(
            self.node.clone(),
            store.to_string(),
            Body {
                id: self.id.fetch_add(1, Ordering::SeqCst).into(),
                in_reply_to: None,
                payload: Payload::Read { key: k },
            },
        );
        let res = self.sync_rpc(msg).context("sending rpc")?;
        match res.body.payload {
            Payload::ReadOk { value } => Ok(value),
            _ => bail!("unexpected return type"),
        }
    }

    fn sync_write(&self, key: impl Into<String>, store: &String, val: usize) -> anyhow::Result<()> {
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

        let res = self.sync_rpc(msg).context("sending rpc for write")?;
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
    fn sync_rpc(&self, msg: Message<Payload>) -> anyhow::Result<Message<Payload>> {
        let (tx, rx) = oneshot::channel::<Message<Payload>>();

        // register transmitter
        self.rpc.lock().unwrap().insert(msg.body.id.unwrap(), tx);
        msg.send(&self.output).context("sending rpc")?;
        eprintln!(
            "Sent|> :dest=>{}, :src=>{}, :body=>[:type=>{:?}, :in_reply_to=>{:?}, :msg_id=>{:?}]",
            msg.dst, msg.src, msg.body.payload, msg.body.in_reply_to, msg.body.id
        );
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
    Error {
        code: usize,
        text: String,
    },
    WriteOk,
    Cas {
        key: String,
        from: usize,
        to: usize,
        #[serde(rename = "create_if_not_exists")]
        put: bool,
    },
    CasOk,
}

impl Node<(), Payload> for KafkaLog {
    fn from_init(
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

    // Switched to immutable self to get around borrow checker
    // which wasn't allowing me to share node between threads
    // with Arc in lib.rs
    fn step(&self, input: nazgul::Message<Payload>) -> anyhow::Result<()> {
        if input.body.in_reply_to.is_some() {
            // eprintln!("Found in reply to|> {:?}", input);
            let tx = self
                .rpc
                .lock()
                .unwrap()
                .remove(&input.body.in_reply_to.unwrap())
                .unwrap();
            if let Err(e) = tx.send(input).context("sending rpc") {
                bail!("channel closed: {}", e.to_string());
            }
            return Ok(());
        }

        let i = input.clone();
        let mut reply = input.into_reply(Some(&self.id));
        match reply.body.payload {
            Payload::Send { key, msg } => {
                let latest_key = format!("{}:latest", key);
                let offset = self
                    .sync_read(latest_key.clone(), &self.lin_store)
                    .context("read offset");
                let mut offset = match offset {
                    Ok(o) => o,
                    Err(_) => 1,
                };

                loop {
                    let curr = offset;
                    eprintln!("Curr|> {}", curr);
                    let (prev, now) = (curr - 1, curr as usize);
                    let res = self
                        .sync_cas(latest_key.clone(), &self.lin_store, prev, now, true)
                        .context("cas offset");

                    match res {
                        Ok(_) => break,
                        Err(_) => offset += 1,
                    };
                }

                eprintln!("KafkaLog4|> {:?}", i);
                let msg_key = format!("{}:{}", key, offset);

                self.sync_write(msg_key, &self.seq_store, msg)
                    .context("write msg_key offset")?;

                self.sync_write(latest_key, &self.seq_store, offset)
                    .context("write latest key with offset")?;

                reply.body.payload = Payload::SendOk { offset };
                reply.send(&self.output).context("reply Send")?;
            }
            Payload::Poll { offsets } => {
                let o = offsets.clone();
                let mut resp: HashMap<String, Vec<[usize; 2]>> = HashMap::new();

                for (k, v) in offsets {
                    let mut m = Vec::new();
                    for i in v..(v + 5) {
                        let val = self
                            .sync_read(format!("{}:{}", k, i), &self.seq_store)
                            .context("read msg_key offset");
                        let val = match val {
                            Ok(o) => o,
                            Err(_) => continue,
                        };
                        m.push([i, val]);
                    }
                    resp.insert(k, m);
                }

                eprintln!("Poll|> {:?}, RESP|>{:?}", o, resp);
                reply.body.payload = Payload::PollOk { msgs: resp };
                reply.send(&self.output).context("reply Poll")?;
            }
            Payload::CommitOffsets { offsets } => {
                offsets.into_iter().for_each(|(key, offset)| {
                    let _ = self
                        .sync_write(format!("commit:{}", key), &self.seq_store, offset)
                        .context("write offset");
                });

                reply.body.payload = Payload::CommitOffsetsOk;
                reply.send(&self.output).context("reply CommitOffsets")?;
            }
            Payload::ListCommittedOffsets { keys } => {
                let mut resp = HashMap::new();
                for key in keys {
                    let offset = self
                        .sync_read(format!("commit:{}", key), &self.seq_store)
                        .context("list committed offset");
                    let offset = match offset {
                        Ok(o) => o,
                        Err(_) => 0,
                    };
                    resp.insert(key, offset);
                }

                reply.body.payload = Payload::ListCommittedOffsetsOk { offsets: resp };
                reply
                    .send(&self.output)
                    .context("reply ListCommittedOffsets")?;
            }
            Payload::Error { code, text } => {
                eprintln!("Error {}: {}", code, text);
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
