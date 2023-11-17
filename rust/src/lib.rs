use anyhow::{Context, Ok};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, StdoutLock, Write},
    sync::{Arc, Mutex},
    thread,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload>
where
    Payload: Serialize,
{
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: id.map(|id| {
                    let mid = *id; // mut lets us deref
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    pub fn send(&self, output: &mut std::io::StdoutLock) -> anyhow::Result<()> {
        serde_json::to_writer(&mut *output, self)
            .context("failed to serialize broadcast  reply")?;
        output
            .write_all(b"\n")
            .context("failed to write new line")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
    BroadcastOk,
}

pub trait Node<S, Payload> {
    fn from_init(state: S, init: Init) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(
        &mut self,
        input: Message<Payload>,
        output: &mut StdoutLock,
        tx: &std::sync::mpsc::Sender<MessageAckStatus<Payload>>,
    ) -> anyhow::Result<()>;

    fn get_un_acked_msgs(&self) -> HashMap<usize, Message<Payload>>;
}

pub struct MessageAckStatus<P> {
    pub status: u8,
    pub msg: Message<P>,
}

pub fn main_loop<S, N, P>(init_state: S) -> anyhow::Result<()>
where
    N: Node<S, P> + Clone,
    P: DeserializeOwned + Serialize + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel::<MessageAckStatus<P>>();
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    // let stdout_arc = Arc::new(Mutex::new(std::io::stdout()));
    // let mut stdout = stdout_arc.lock().unwrap().lock();
    let mut stdout = std::io::stdout().lock();

    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("no init messagen received")
            .context("failed to read from stdin")?,
    )
    .context("init message could not be deserialized")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first message should be init");
    };

    let mut node: N = Node::from_init(init_state, init).context("node initialization failed")?;
    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };
    // serde_json::to_writer(&mut stdout, &reply).context("failed to send init message")?;
    // stdout.write_all(b"\n").context("write trailing newline")?;
    reply.send(&mut stdout).context("failed to send message")?;

    // let stdout_clone = Arc::clone(&stdout_arc);
    // thread::spawn(move || -> anyhow::Result<()> {
    //     let mut l_msgs: HashMap<usize, Message<P>> = HashMap::new();

    //     thread::spawn(move || -> anyhow::Result<()> {
    //         let mut stdout_lock = stdout_clone.lock().unwrap().lock();
    //         for (_, n) in &l_msgs {
    //             n.send(&mut stdout_lock)
    //                 .context("failed to resend message")?;
    //         }
    //         Ok(())
    //     });
    //     for m in rx {
    //         if m.status == 1 {
    //             let j = l_msgs.get(&m.msg.body.id.unwrap());
    //             if j.is_some() {
    //                 l_msgs.remove(&m.msg.body.id.unwrap());
    //             }
    //             continue;
    //         }
    //     }
    //     Ok(())
    // });

    for line in stdin {
        let line = line.context("Maelstrom input from STDIN could not be read")?;
        let input: Message<P> = serde_json::from_str(&line)
            .context("Maelstrom input from STDIN could not deserialised")?;
        node.step(input, &mut stdout, &tx)
            .context("Node step function failed")?;

        // for msg in node.get_un_acked_msgs() {
        //     msg.1
        //         .send(&mut stdout)
        //         .context("failed to resend message")?;
        // }
    }

    Ok(())
}
