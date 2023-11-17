use anyhow::{Context, Ok};
use nazgul::*;

use std::{collections::HashMap, thread, time::Duration};

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
    BroadcastOk {
        in_reply_to: Option<usize>,
    },
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

    fn from_init(_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
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
        tx: &std::sync::mpsc::Sender<MessageAckStatus<Payload>>,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                reply.body.payload = Payload::BroadcastOk {
                    in_reply_to: reply.body.in_reply_to,
                };
                reply.send(output).context("failed to send message")?;

                if !self.messages.contains(&message) {
                    self.messages.push(message);
                    for node in &self.neighbors {
                        if node.as_str() == self.node.as_str() {
                            continue;
                        }
                        let broad_msg = Message {
                            src: self.node.clone(),
                            dst: node.to_string(),
                            body: Body {
                                id: Some(self.id),
                                in_reply_to: None,
                                payload: Payload::Broadcast { message },
                            },
                        };
                        self.id += 1;
                        broad_msg
                            .send(output)
                            .context(format!("failed to send message to node: {node}"))?;

                        // tx.send(MessageAckStatus {
                        //     status: 0,
                        //     msg: broad_msg.clone(),
                        // })
                        // .context("failed to send")?;

                        self.waiting_for_ack
                            .insert(broad_msg.body.id.unwrap(), broad_msg);
                    }
                }
            }
            Payload::BroadcastOk { in_reply_to } => {
                // let Some(msg_id) = in_reply_to else {
                //     return Ok(());
                // };
                // if self.waiting_for_ack.contains_key(&msg_id) {
                //     let msg = self.waiting_for_ack.remove(&msg_id);
                //     tx.send(MessageAckStatus {
                //         status: 1,
                //         msg: msg.unwrap(),
                //     })
                //     .context("failed to send")?;
                // }
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
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
