use nazgul::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io::{StdoutLock, Write},
    sync::mpsc::Sender,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

#[derive(Clone)]
struct EchoNode {
    id: usize,
}
// { "src": "c1", "dest": "n3", "body": { "type": "init", "msg_id": 1, "node_id": "n3", "node_ids": ["n1", "n2", "n3"] } }
// {"src":"n3","dest":"n2","body":{"msg_id":1000,"type":"topology","topology":{"n1": ["n3"],"n2": ["n1"],"n3": ["n1","n2"]}}}
// { "src": "n2", "dest": "n3", "body": { "msg_id":1, "type": "broadcast", "message": 2 } }
// {"src":"n3","dest":"n2","body":{"msg_id":2,"type":"broadcast_ok",}}
// {"src":"n2","dest":"n3","body":{"msg_id":1,"in_reply_to":4,"type":"broadcast_ok"}}
// { "src": "n2", "dest": "n3", "body": { "msg_id":1, "type": "broadcast", "message": 2 }
// {"src":"n2","dest":"n3","body":{"msg_id":1,"in_reply_to":4,"type":"broadcast_ok"}}
// {"src":"n3","dest":"n2","body":{"msg_id":3,"in_reply_to":1,"type":"broadcast_ok"}}
impl Node<(), Payload> for EchoNode {
    fn get_un_acked_msgs(&self) -> std::collections::HashMap<usize, Message<Payload>> {
        HashMap::new()
    }

    fn from_init(_state: (), _init: Init, _tx: Sender<Message<Payload>>) -> anyhow::Result<Self> {
        Ok(EchoNode { id: 1 })
    }

    fn step(&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let mut reply = input.into_reply(Some(&mut self.id));
        match reply.body.payload {
            Payload::Echo { echo } => {
                reply.body.payload = Payload::EchoOk { echo };
                serde_json::to_writer(&mut *output, &reply)
                    .context("failed to serialize response")?;
                output.write_all(b"\n").context("writing new line")?;
            }
            Payload::EchoOk { .. } => {}
        }
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<_, EchoNode, _>(())
}
