use anyhow::Context;
use nazgul::*;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

struct BroadcastNode {
    id: usize,
    node: String,
    messages: Vec<usize>,
    neighbors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Broadcast {
        message: usize,
    },
    BroadcastOk,
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
    fn from_init(_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(BroadcastNode {
            id: 1,
            node: init.node_id,
            messages: Vec::new(),
            neighbors: Vec::new(),
        })
    }

    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Broadcast { message } => {
                self.messages.push(message);
                reply.body.payload = Payload::BroadcastOk;
                reply.send(output).context("failed to send message")?;

                for node in &self.neighbors {
                    self.id += 1;
                    let broad_msg = Message {
                        src: self.node.clone(),
                        dst: node.to_string(),
                        body: Body {
                            id: Some(self.id),
                            in_reply_to: None,
                            payload: Payload::Broadcast { message },
                        },
                    };
                    broad_msg
                        .send(output)
                        .context(format!("failed to send message to node: {node}"))?
                }
            }
            Payload::BroadcastOk => {}
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
