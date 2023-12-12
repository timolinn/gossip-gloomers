use std::collections::{HashMap, LinkedList};

use anyhow::Context;
use nazgul::{main_loop, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
struct KafkaLog {
    id: usize,
    logs: HashMap<String, LinkedList<Log>>,
    commit_offsets: HashMap<String, usize>,
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
}

impl Node<(), Payload> for KafkaLog {
    fn from_init(
        _state: (),
        _init: nazgul::Init,
        _tx: std::sync::mpsc::Sender<nazgul::Message<Payload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            id: 1,
            logs: HashMap::new(),
            commit_offsets: HashMap::new(),
        })
    }

    fn step(
        &mut self,
        input: nazgul::Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Send { key, msg } => {
                let res = self.logs.entry(key).or_default();
                res.push_back(Log::from(msg));

                reply.body.payload = Payload::SendOk {
                    offset: res.len() - 1,
                };
                reply.send(output).context("reply Send")?;
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
                reply.send(output).context("reply Poll")?;
            }
            Payload::CommitOffsets { offsets } => {
                offsets.into_iter().for_each(|(key, offset)| {
                    self.commit_offsets
                        .entry(key.clone())
                        .and_modify(|o| *o = offset)
                        .or_insert(offset);
                });

                reply.body.payload = Payload::CommitOffsetsOk;
                reply.send(output).context("reply CommitOffsets")?;
            }
            Payload::ListCommittedOffsets { keys } => {
                let mut resp = HashMap::new();
                keys.into_iter().for_each(|key| {
                    if let Some(offset) = self.commit_offsets.get(&key) {
                        resp.insert(key, *offset);
                    }
                });
                reply.body.payload = Payload::ListCommittedOffsetsOk { offsets: resp };
                reply.send(output).context("reply ListCommittedOffsets")?;
            }
            Payload::PollOk { .. }
            | Payload::SendOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, KafkaLog, _>(())
}
