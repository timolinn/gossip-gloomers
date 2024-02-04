use anyhow::{Context, Ok};
use nazgul::*;

use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, mpsc::Sender, Mutex},
    thread,
    time::Duration,
};

use serde::{Deserialize, Serialize};

struct BroadcastNode {
    id: AtomicUsize,
    node: String,
    messages: Vec<usize>,
    neighbors: Vec<String>,
    waiting_for_ack: HashMap<usize, Message<Payload>>,
    output: Mutex<std::io::Stdout>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    BroadcastUnAcked,
    Read,
    ReadOk {
        messages: Vec<usize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

impl Node<(), Payload> for BroadcastNode {
    fn from_init(_state: (), init: Init, tx: Sender<Message<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node_id = init.node_id.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(6000));
            let msg = Message::new(
                node_id.as_str().to_string(),
                node_id.as_str().to_string(),
                Body {
                    id: None,
                    in_reply_to: None,
                    payload: Payload::BroadcastUnAcked,
                },
            );
            if tx.send(msg).context("send input").is_err() {
                break;
            }
        });
        Ok(BroadcastNode {
            id: AtomicUsize::new(1),
            node: init.node_id,
            messages: Vec::new(),
            neighbors: Vec::new(),
            waiting_for_ack: HashMap::new(),
            output: Mutex::new(std::io::stdout()),
        })
    }

    fn step(&self, input: Message<Payload>) -> anyhow::Result<()> {
        let src = input.src.clone();
        let mut reply = input.into_reply(Some(&self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                reply.body.payload = Payload::BroadcastOk;
                reply.send(&self.output).context("failed to send message")?;

                if !self.messages.contains(&message) {
                    self.messages.push(message);
                    for neighbor in &self.neighbors {
                        if neighbor.as_str() == self.node.as_str() || neighbor.as_str() == src {
                            continue;
                        }
                        let broad_msg = Message {
                            src: self.node.clone(),
                            dst: neighbor.to_string(),
                            body: Body {
                                id: Some(self.id.load(std::sync::atomic::Ordering::Relaxed)),
                                in_reply_to: None,
                                payload: Payload::Broadcast { message },
                            },
                        };
                        self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        broad_msg
                            .send(&self.output)
                            .context(format!("failed to send message to node: {neighbor}"))?;

                        // std::mem::swap(&mut broad_msg.src, &mut broad_msg.dst); // into_reply will swap them
                        // TODO: needs refactor to use mutable ref
                        // switched to immutable self because of kafka-log
                        // consider smart pointers to get mutable access
                        self.waiting_for_ack
                            .insert(broad_msg.body.id.unwrap(), broad_msg);
                    }
                }
            }
            Payload::BroadcastOk => {
                let Some(msg_id) = reply.body.in_reply_to else {
                    return Ok(());
                };
                if self.waiting_for_ack.contains_key(&msg_id) {
                    // TODO: needs refactor to use mutable ref
                    // switched to immutable self because of kafka-log
                    // consider smart pointers to get mutable access
                    self.waiting_for_ack
                        .remove(&msg_id)
                        .context("removing message from waiting to ack map")?;
                }
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                reply.send(&self.output).context("failed to send message")?;
            }
            Payload::Topology { mut topology } => {
                self.neighbors = topology.remove(&self.node).unwrap_or_else(|| {
                    panic!("could not retrieve topology for node: {}", &self.node)
                });
                reply.body.payload = Payload::TopologyOk;
                reply.send(&self.output).context("failed to send message")?;
            }
            Payload::BroadcastUnAcked => {
                // let l = self
                //     .waiting_for_ack
                //     .iter()
                //     .map(|x| x.1)
                //     .collect::<Vec<&Message<Payload>>>();
                for m in self.waiting_for_ack.values() {
                    m.send(&self.output).context("failed to send message")?;
                }
            }
            Payload::TopologyOk | Payload::ReadOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
