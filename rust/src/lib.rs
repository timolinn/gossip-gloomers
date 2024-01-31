use anyhow::{Context, Ok};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    io::{BufRead, StdoutLock, Write},
    sync::{mpsc::Sender, Mutex},
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
    Payload: Serialize + Debug,
{
    pub fn new(src: String, dst: String, body: Body<Payload>) -> Self {
        Self { src, dst, body }
    }
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

    pub fn send(&self, output: &Mutex<std::io::Stdout>) -> anyhow::Result<()> {
        let out = output.lock().unwrap();
        let data = serde_json::to_string(self).context("failed to serialize broadcast reply")?;
        out.lock()
            .write_all(data.as_bytes())
            .context("failed to write new line")?;
        out.lock()
            .write_all(b"\n")
            .context("failed to write new line")?;
        // eprintln!(
        //     "Sent|> :dest=>{}, :src=>{}, :body=>[:type=>{:?}, :in_reply_to=>{:?}, :msg_id=>{:?}]",
        //     self.dst, self.src, self.body.payload, self.body.in_reply_to, self.body.id
        // );
        Ok(())
    }

    pub fn send_shared(&self, output: &mut std::io::Stdout) -> anyhow::Result<()> {
        eprintln!("SENDING shared {:?}", self.body.id);
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
}

pub trait Node<S, Payload> {
    fn from_init(state: S, init: Init, tx: Sender<Message<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized;

    fn step(&mut self, input: Message<Payload>) -> anyhow::Result<()>;
}

pub trait KV<T>: Send + Sync {
    /// Read returns the value for a given key in the key/value store.
    /// Returns an RPCError error with a KeyDoesNotExist code if the key does not exist.
    fn sync_read(&self, key: impl Into<String>, store: &String) -> anyhow::Result<T>
    where
        T: Deserialize<'static> + Send;

    /// Write overwrites the value for a given key in the key/value store.
    fn sync_write(&self, key: impl Into<String>, store: &String, val: T) -> anyhow::Result<()>
    where
        T: Serialize + Send;

    /// compare and set (CAS) updates the value for a key if its current value matches the
    /// previous value. Creates the key if it is not exist is requested.
    ///
    /// Returns an RPCError with a code of PreconditionFailed if the previous value
    /// does not match. Return a code of KeyDoesNotExist if the key did not exist.
    fn sync_cas(
        &self,
        key: impl Into<String>,
        store: &String,
        from: T,
        to: T,
        put: bool,
    ) -> anyhow::Result<()>
    where
        T: Serialize + Deserialize<'static> + Send;
}

pub fn main_loop<S, N, P>(init_state: S) -> anyhow::Result<()>
where
    N: Node<S, P>,
    P: DeserializeOwned + Serialize + Send + 'static + std::marker::Sync,
{
    let (tx, rx) = std::sync::mpsc::channel::<Message<P>>();
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let stdout = Mutex::new(std::io::stdout());

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

    let mut node: N =
        Node::from_init(init_state, init, tx.clone()).context("node initialization failed")?;
    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    reply.send(&stdout).context("failed to send message")?;

    drop(stdin);
    let jh = thread::spawn(move || -> anyhow::Result<()> {
        for line in std::io::stdin().lock().lines() {
            let line = line.context("Maelstrom input from STDIN could not be read")?;
            let input: Message<P> = serde_json::from_str(&line)
                .context("Maelstrom input from STDIN could not deserialised")?;

            tx.send(input).context("send input")?;
        }

        Ok(())
    });

    for m in rx {
        node.step(m).context("Node step function failed")?;
    }

    jh.join()
        .expect("stdin thread panicked")
        .context("stdin thread err'd")?;

    Ok(())
}
