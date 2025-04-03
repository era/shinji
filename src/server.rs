use crate::id::Id;
use crate::node::{Node, NodeState};
use crate::storage::Value;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::time::{sleep, Duration};
use warp::Filter;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RequestStore {
    pub sender: NodeState,
    pub data: Vec<u8>,
    pub id: Id,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RequestFind {
    pub sender: NodeState,
    pub id: Id,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum ResponseFind {
    Servers(Vec<NodeState>),
    Value(Vec<u8>, i64),
}

pub struct Server {
    // all requests are serialized, if this was a production code
    // we should move the lock to inside the node and the storage
    // and try to shard the locks, so that multiple non-related
    // requests can go through
    node: Arc<RwLock<Node>>,
    state: NodeState,
    port: u32,
}

impl Server {
    pub fn new(public_ip: SocketAddr, port: u32) -> Self {
        // using sha1 just because this is a learning library
        // and sha1 generates hashes with the right size of the key
        // in the original paper
        // this should not be used in any security critical capacity.
        let mut hasher = Sha1::new();
        let bytes = public_ip.to_string();
        let bytes = bytes.as_bytes();
        hasher.update(bytes);

        let id = Id(hasher.finalize().into());
        let state = NodeState {
            id,
            address: public_ip,
        };

        let node = Node::new(state.clone(), 16, 10);
        let node = Arc::new(RwLock::new(node));

        Self { node, port, state }
    }

    ///  spawn two tasks:
    ///  1. a thread to keep cleaning old values stored in-memory
    ///  2. the webserver
    pub fn spawn_tasks(&self) -> Result<(), Box<dyn Error>> {
        let tick = self.node.clone();
        tokio::task::spawn(async move {
            loop {
                // cleans all data older than 5 minutes
                tick.write().unwrap().tick(5);
                sleep(Duration::from_secs(60)).await;
            }
        });

        let node = self.node.clone();
        let port = self.port;
        let address: core::net::SocketAddr = format!("127.0.0.1:{port}").parse()?;

        tokio::task::spawn(async move {
            let node_warp = warp::any().map(move || node.clone());

            let store_value = warp::path!("store")
                .and(warp::body::json())
                .and(node_warp.clone())
                .map(|value: RequestStore, node: Arc<RwLock<Node>>| {
                    let mut node = node.write().unwrap();
                    node.new_contact(value.sender);
                    node.store(value.id, value.data);
                    warp::reply::with_status("{}", warp::http::StatusCode::CREATED)
                });
            let find_value = warp::path!("find_value")
                .and(warp::body::json())
                .and(node_warp.clone())
                .map(|value: RequestFind, node: Arc<RwLock<Node>>| {
                    let mut node = node.write().unwrap();
                    node.new_contact(value.sender);

                    let response = node.find_value(&value.id, 10);
                    let response = match response {
                        either::Either::Right(s) => ResponseFind::Servers(s),
                        either::Either::Left(v) => ResponseFind::Value(v.data, v.timestamp),
                    };
                    warp::reply::json(&response)
                });
            let find_node = warp::path!("find_node")
                .and(warp::body::json())
                .and(node_warp.clone())
                .map(|value: RequestFind, node: Arc<RwLock<Node>>| {
                    let mut node = node.write().unwrap();
                    node.new_contact(value.sender);

                    let response = ResponseFind::Servers(node.find_node(&value.id, 10));
                    warp::reply::json(&response)
                });

            let routes = store_value.or(find_value).or(find_node);
            warp::serve(routes).run(address).await;
        });

        Ok(())
    }

    /// get Value from the network (looks at local storage first)
    pub async fn get(&self, id: &Id) -> Option<Value> {
        let node = self.node.read().unwrap();
        match node.find_value(id, 5) {
            either::Either::Left(value) => Some(value),
            either::Either::Right(servers) => {
                // drop lock, since we don't need to hold it
                // while fetching data from other servers
                drop(node);
                try_finding_value(servers, id).await
            }
        }
    }

    /// publish a new (Id, Value) in the network
    pub async fn publish(&self, id: Id, value: Vec<u8>) {
        let node = self.node.read().unwrap();
        let servers = node.find_node(&id, 5);
        drop(node);
        publish(self.state.clone(), servers, id, value).await;
    }
}

async fn publish(sender: NodeState, servers: Vec<NodeState>, id: Id, data: Vec<u8>) {
    let request = RequestStore { id, sender, data };
    for server in servers {
        let request = request.clone();
        tokio::task::spawn(async move {
            let client = reqwest::Client::new();
            let res = client
                .post(format!("http://{}/store", server.address))
                .json(&request)
                .send()
                .await;
            tracing::info!(for_server = %server.address, result = ?res, "publish value");
        });
    }
    todo!()
}

async fn try_finding_value(servers: Vec<NodeState>, id: &Id) -> Option<Value> {
    todo!()
}
