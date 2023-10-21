use anyhow::Context;
use nazgul::*;

use std::{collections::HashMap, io::Write};

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
                serde_json::to_writer(&mut *output, &reply)
                    .context("failed to serialize broadcast  reply")?;
                output
                    .write_all(b"\n")
                    .context("failed to write new line")?;
            }
            Payload::BroadcastOk => {}
            Payload::Read => {
                reply.body.payload = Payload::ReadOk {
                    messages: self.messages.clone(),
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("failed to serialize read reply")?;
                output
                    .write_all(b"\n")
                    .context("failed to write new line")?;
            }
            Payload::ReadOk { .. } => {}
            Payload::Topology { topology } => {
                self.neighbors = topology[&self.node].clone();
                reply.body.payload = Payload::TopologyOk;
                serde_json::to_writer(&mut *output, &reply)
                    .context("failed to serialize topology reply")?;
                output
                    .write_all(b"\n")
                    .context("failed to write new line")?;
            }
            Payload::TopologyOk => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, BroadcastNode, _>(())
}
