use anyhow::{Context, Ok};
use nazgul::*;

use std::{collections::HashMap, sync::mpsc::Sender, thread, time::Duration};

use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct BroadcastNode {
    id: usize,
    node: String,
    messages: Vec<usize>,
    neighbors: Vec<String>,
    waiting_for_ack: HashMap<usize, Message<Payload>>,
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
    fn get_un_acked_msgs(&self) -> HashMap<usize, Message<Payload>> {
        self.waiting_for_ack.clone()
    }

    fn from_init(_state: (), init: Init, tx: Sender<Message<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node_id = init.node_id.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(1000));
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
            id: 1,
            node: init.node_id,
            messages: Vec::new(),
            neighbors: Vec::new(),
            waiting_for_ack: HashMap::new(),
        })
    }

    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let src = input.src.clone();
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
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
                                id: Some(self.id),
                                in_reply_to: None,
                                payload: Payload::Broadcast { message },
                            },
                        };
                        self.id += 1;
                        broad_msg
                            .send(output)
                            .context(format!("failed to send message to node: {neighbor}"))?;

                        // std::mem::swap(&mut broad_msg.src, &mut broad_msg.dst); // into_reply will swap them
                        self.waiting_for_ack
                            .insert(broad_msg.body.id.unwrap(), broad_msg);
                    }

                    reply.body.payload = Payload::BroadcastOk;
                    reply.send(output).context("failed to send message")?;
                }
            }
            Payload::BroadcastOk => {
                let Some(msg_id) = reply.body.in_reply_to else {
                    return Ok(());
                };
                if self.waiting_for_ack.contains_key(&msg_id) {
                    self.waiting_for_ack
                        .remove(&msg_id)
                        .context("removing message from waiting to ack map")?;
                }
            }
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                reply.send(output).context("failed to send message")?;
            }
            Payload::ReadOk { .. } => {}
            Payload::Topology { topology } => {
                self.neighbors = topology[&self.node].clone();
                reply.body.payload = Payload::TopologyOk;
                reply.send(output).context("failed to send message")?;
            }
            Payload::TopologyOk => {}
            Payload::BroadcastUnAcked => {
                for m in self.waiting_for_ack.values() {
                    let msg = m.clone();
                    msg.send(output).context("failed to send message")?;
                }
            } // _ => {
              //     // if reply.body.payload == Payload::Init {
              //     //     return Ok(());
              //     // }
              //     eprintln!("not supported: {:?}", reply.body.payload)
              // }
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
