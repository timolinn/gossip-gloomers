use std::{
    collections::{HashMap, LinkedList},
    sync::Mutex,
};

use anyhow::Context;
use nazgul::{main_loop, Body, Message, Node, KV};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct KafkaLog {
    id: usize,
    logs: HashMap<String, LinkedList<Log>>,
    commit_offsets: HashMap<String, usize>,
    node: String,
    lin_store: String,
    seq_store: String,
    rpc: Mutex<HashMap<usize, oneshot::Receiver<Message<Payload>>>>,
    output: Mutex<std::io::Stdout>,
}

impl nazgul::KV for KafkaLog {
    fn cas<T>(&self, key: impl Into<String>, from: T, to: T, put: bool) -> anyhow::Result<()>
    where
        T: serde::Serialize + serde::Deserialize<'static> + Send,
    {
        Ok(())
    }

    fn read<T>(&mut self, key: impl Into<String>) -> anyhow::Result<T> {
        let id = Some(&mut self.id);
        let msg = Message::new(
            self.node.clone(),
            store,
            Body {
                // id: self.id.fetch_add(1, Ordering::SeqCst).into(),
                id: id.map(|id| {
                    let mid = *id; // mut lets us deref
                    *id += 1;
                    mid
                }),
                in_reply_to: None,
                payload: Payload::Read { key: key.into() },
            },
        );
        Ok(())
    }

    fn write<T>(&self, key: impl Into<String>, val: T) -> anyhow::Result<()>
    where
        T: serde::Serialize + Send,
    {
        Ok(())
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
    fn sync_rpc(
        &self,
        msg: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<Message<Payload>> {
        let (tx, rx) = oneshot::channel::<Message<Payload>>();
        self.rpc.lock().unwrap().insert(msg.body.id.unwrap(), rx);
        msg.send(&self.output).context("sending rpc");
        let res = rx.recv().unwrap();
        Ok(res)
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
        from: String,
        to: String,
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
            id: 1,
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
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Send { key, msg } => {
                let res = self.logs.entry(key).or_default();
                res.push_back(Log::from(msg));

                reply.body.payload = Payload::SendOk {
                    offset: res.len() - 1,
                };
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
