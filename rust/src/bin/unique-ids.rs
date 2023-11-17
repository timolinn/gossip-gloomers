use std::{collections::HashMap, io::Write};

use anyhow::{Context, Ok};
use nazgul::*;

use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct UniqueIdNode {
    id: usize,
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
    fn get_un_acked_msgs(&self) -> std::collections::HashMap<usize, Message<Payload>> {
        HashMap::new()
    }

    fn from_init(_state: (), init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueIdNode {
            id: 1,
            node: init.node_id,
        })
    }

    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut std::io::StdoutLock,
        _tx: &std::sync::mpsc::Sender<MessageAckStatus<Payload>>,
    ) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Generate => {
                reply.body.payload = Payload::GenerateOk {
                    guid: format!("{}-{}", self.node, self.id),
                };
                serde_json::to_writer(&mut *output, &reply)
                    .context("failed to serialize response")?;
                output.write_all(b"\n").context("failed to write newline")?;
            }
            Payload::GenerateOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, UniqueIdNode, _>(())
}
