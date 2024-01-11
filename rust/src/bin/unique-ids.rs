use std::{
    io::Write,
    sync::{mpsc::Sender, Mutex},
};

use anyhow::{Context, Ok};
use nazgul::*;

use serde::{Deserialize, Serialize};

struct UniqueIdNode {
    id: usize,
    output: Mutex<std::io::Stdout>,
    node: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Generate,
    GenerateOk {
        #[serde(rename = "id")]
        guid: String,
    },
}

impl Node<(), Payload> for UniqueIdNode {
    fn from_init(_state: (), init: Init, _tx: Sender<Message<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueIdNode {
            id: 1,
            output: Mutex::new(std::io::stdout()),
            node: init.node_id,
        })
    }

    fn step(&mut self, input: Message<Payload>) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate => {
                reply.body.payload = Payload::GenerateOk {
                    guid: format!("{}-{}", self.node, self.id),
                };
                reply
                    .send(&self.output)
                    .context("failed to serialize response")?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueIdNode, _>(())
}
