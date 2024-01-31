use std::{collections::HashMap, sync::Mutex, thread, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use nazgul::{main_loop, Body, Message, Node};
use serde::{Deserialize, Serialize};

struct GrowOnlyCounter {
    id: usize,
    node: String,
    count: usize,
    node_ids: Vec<String>,
    node_values: HashMap<String, usize>,
    output: Mutex<std::io::Stdout>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    ServerRead,
    ServerReadOk { value: usize },
}

#[async_trait]
impl Node<(), Payload> for GrowOnlyCounter {
    async fn from_init(
        _state: (),
        init: nazgul::Init,
        tx: std::sync::mpsc::Sender<nazgul::Message<Payload>>,
    ) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node = Self {
            id: 1,
            node: init.node_id.clone(),
            count: 0,
            node_ids: init
                .node_ids
                .into_iter()
                .filter(|x| *x != init.node_id)
                .collect(),
            node_values: HashMap::new(),
            output: Mutex::new(std::io::stdout()),
        };
        let nid = node.node.clone();
        let nids = node.node_ids.clone();
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(5000));
            for n in nids.clone() {
                let msg = Message {
                    src: n,
                    dst: nid.to_string(),
                    body: Body {
                        id: None,
                        in_reply_to: None,
                        payload: Payload::ServerRead,
                    },
                };
                if tx.send(msg).is_err() {
                    break;
                }
            }
        });
        Ok(node)
    }

    async fn step(&mut self, input: nazgul::Message<Payload>) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Add { delta } => {
                self.count += delta;
                reply.body.payload = Payload::AddOk;
                reply.send(&self.output).context("sending AddOk")?;
            }
            Payload::Read => {
                let mut count = self.count;
                for nv in self.node_values.values() {
                    count += nv;
                }
                reply.body.payload = Payload::ReadOk { value: count };
                reply.send(&self.output).context("sending ReadOk")?;
            }
            Payload::ServerRead => {
                eprintln!(
                    "SENDING ServerRead from {}, going to {}, with value {}",
                    reply.src, reply.dst, self.count
                );
                reply.body.payload = Payload::ServerReadOk { value: self.count };
                reply.send(&self.output).context("sending ServerReadOk")?;
            }
            Payload::ServerReadOk { value } => {
                eprintln!(
                    "RECEIVED ServerOk from {}, going to {}, with value {}",
                    reply.dst, reply.src, value
                );
                // we don't want to send an actual message
                // reply.dst is the actual src
                self.node_values.insert(reply.dst, value);
            }
            Payload::AddOk | Payload::ReadOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, GrowOnlyCounter, _>(())
}
