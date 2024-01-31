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
        _put: bool,
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
        let msg = Message::new(
            self.node.clone(),
            store.to_string(),
            Body {
                id: self.id.fetch_add(1, Ordering::SeqCst).into(),
                in_reply_to: None,
                payload: Payload::Read { key: key.into() },
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

    fn step(&mut self, input: nazgul::Message<Payload>) -> anyhow::Result<()> {
        let mut rpc = self.rpc.lock().unwrap();
        if rpc.contains_key(&input.body.id.unwrap()) {
            let tx = rpc.remove(&input.body.id.unwrap()).unwrap();
            if let Err(e) = tx.send(input).context("sending rpc") {
                bail!("channel closed: {}", e.to_string());
            }
            return Ok(());
        }

        let mut reply = input.into_reply(Some(self.id.get_mut()));
        match reply.body.payload {
            Payload::Send { key, msg } => {
                // let res = self.logs.entry(key.clone()).or_default();
                // res.push_back(Log::from(msg));

                let offset = self
                    .sync_read(key.clone(), &self.lin_store)
                    .context("Read Offset");
                let offset = match offset {
                    Ok(o) => o,
                    Err(_) => 0,
                };

                self.sync_write(key, &self.lin_store, msg)
                    .context("Write Offset")?;

                reply.body.payload = Payload::SendOk { offset };
                reply.send(&self.output).context("reply Send")?;
            }
            Payload::Poll { offsets } => {
                let o = offsets.clone();
                let mut resp = HashMap::new();

                for (k, v) in offsets {
                    let kl = self.logs.entry(k.clone()).or_default();
                    let res = kl
                        .iter()
                        .skip(v)
                        .take(3)
                        .enumerate()
                        .map(|l| [v + l.0, l.1.value])
                        .collect();
                    resp.insert(k, res);
                }

                eprintln!("Poll|> {:?}, RESP|>{:?}", o, resp);
                reply.body.payload = Payload::PollOk { msgs: resp };
                reply.send(&self.output).context("reply Poll")?;
            }
            Payload::CommitOffsets { offsets } => {
                offsets.into_iter().for_each(|(key, offset)| {
                    self.commit_offsets
                        .entry(key.clone())
                        .and_modify(|o| *o = offset)
                        .or_insert(offset);
                });

                reply.body.payload = Payload::CommitOffsetsOk;
                reply.send(&self.output).context("reply CommitOffsets")?;
            }
            Payload::ListCommittedOffsets { keys } => {
                let mut resp = HashMap::new();
                keys.into_iter().for_each(|key| {
                    if let Some(offset) = self.commit_offsets.get(&key) {
                        resp.insert(key, *offset);
                    }
                });
                reply.body.payload = Payload::ListCommittedOffsetsOk { offsets: resp };
                reply
                    .send(&self.output)
                    .context("reply ListCommittedOffsets")?;
            }
            Payload::PollOk { .. }
            | Payload::SendOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. } => {}
            Payload::Read { key } => todo!(),
            Payload::ReadOk { value } => todo!(),
            Payload::Write { key, value } => todo!(),
            Payload::WriteOk => todo!(),
            Payload::Cas { key, from, to } => todo!(),
            Payload::CasOk => todo!(),
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaLog, _>(())
}
