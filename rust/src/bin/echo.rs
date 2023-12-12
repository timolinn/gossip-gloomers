use nazgul::*;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::{
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
// c25 → n0 {:type "topology",
//      :topology {"n2" ["n7" "n3" "n1"],
//      "n21" ["n16" "n22" "n20"],
//      "n23" ["n18" "n24" "n22"],
//      "n5" ["n10" "n0" "n6"],
//      "n8" ["n13" "n3" "n9" "n7"],
//      "n6" ["n11" "n1" "n7" "n5"],
//      "n19" ["n24" "n14" "n18"],
//      "n14" ["n19" "n9" "n13"],
//      "n17" ["n22" "n12" "n18" "n16"],
//      "n1" ["n6" "n2" "n0"],
//      "n24" ["n19" "n23"],
//      "n4" ["n9" "n3"],
//      "n0" ["n5" "n1"],
//      "n18" ["n23" "n13" "n19" "n17"],
//      "n9" ["n14" "n4" "n8"],
//      "n10" ["n15" "n5" "n11"],
//      "n22" ["n17" "n23" "n21"],
//      "n15" ["n20" "n10" "n16"],
//      "n13" ["n18" "n8" "n14" "n12"],
//      "n3" ["n8" "n4" "n2"],
//      "n12" ["n17" "n7" "n13" "n11"],
//      "n11" ["n16" "n6" "n12" "n10"],
//      "n16" ["n21" "n11" "n17" "n15"],
//      "n7" ["n12" "n2" "n8" "n6"],
//      "n20" ["n15" "n21"]}, :msg_id 1}
// c26 -> n1 {:type "topology",
//      :topology {"n2" ["n7" "n3" "n1"],
//     "n21" ["n16" "n22" "n20"],
//     "n23" ["n18" "n24" "n22"],
//     "n5" ["n10" "n0" "n6"],
//     "n8" ["n13" "n3" "n9" "n7"],
//     "n6" ["n11" "n1" "n7" "n5"],
//     "n19" ["n24" "n14" "n18"],
//     "n14" ["n19" "n9" "n13"],
//     "n17" ["n22" "n12" "n18" "n16"],
//     "n1" ["n6" "n2" "n0"],
//     "n24" ["n19" "n23"],
//     "n4" ["n9" "n3"],
//     "n0" ["n5" "n1"],
//     "n18" ["n23" "n13" "n19" "n17"],
//     "n9" ["n14" "n4" "n8"],
//     "n10" ["n15" "n5" "n11"],
//     "n22" ["n17" "n23" "n21"],
//     "n15" ["n20" "n10" "n16"],
//     "n13" ["n18" "n8" "n14" "n12"],
//     "n3" ["n8" "n4" "n2"],
//     "n12" ["n17" "n7" "n13" "n11"],
//     "n11" ["n16" "n6" "n12" "n10"],
//     "n16" ["n21" "n11" "n17" "n15"],
//     "n7" ["n12" "n2" "n8" "n6"],
//     "n20" ["n15" "n21"]},
// :msg_id 1}
// {"src":"c1","dest":"n0","body":{"msg_id":3,"in_reply_to":1,"type":"send", "key":"a","msg": 2}}
// {"src":"c2","dest":"n0","body":{"msg_id":4,"in_reply_to":1,"type":"send", "key":"a","msg": 20}}
// {"src":"c2","dest":"n0","body":{"msg_id":4,"in_reply_to":1,"type":"send", "key":"a","msg": 200}}
// {"src":"c1","dest":"n0","body":{"msg_id":3,"in_reply_to":1,"type":"send", "key":"b","msg": 2}}
// {"src":"c2","dest":"n0","body":{"msg_id":4,"in_reply_to":1,"type":"send", "key":"b","msg": 20}}
// {"src":"c2","dest":"n0","body":{"msg_id":4,"in_reply_to":1,"type":"poll", "offsets": {"b": 1,"a": 0}}}
// {"src":"c1","dest":"n0","body":{"msg_id":3,"in_reply_to":1,"type":"commit_offsets", "offsets": {}}}

impl Node<(), Payload> for EchoNode {
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
