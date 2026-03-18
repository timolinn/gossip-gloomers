use nazgul::{main_loop, Init, Message, Node};
use std::sync::atomic::AtomicUsize;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::sync::{mpsc::Sender, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    id: AtomicUsize,
    output: Mutex<std::io::Stdout>,
}

impl Node<(), Payload> for EchoNode {
    fn from_init(_state: (), _init: Init, _tx: Sender<Message<Payload>>) -> anyhow::Result<Self> {
        Ok(EchoNode {
            id: AtomicUsize::new(1),
            output: Mutex::new(std::io::stdout()),
        })
    }

    fn step(&self, input: Message<Payload>) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                reply
                    .send(&self.output)
                    .context("failed to serialize response")?;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _>(())
}
