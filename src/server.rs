use crate::id::Id;
use crate::node::{Node, NodeState};
use crate::storage::Value;
use serde::de::DeserializeOwned;
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

pub struct Server {
    node: Arc<RwLock<Node>>,
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

        let node = Node::new(state, 16, 10);
        let node = Arc::new(RwLock::new(node));

        Self { node, port }
    }

    pub fn spawn_tasks(&self) -> Result<(), Box<dyn Error>> {
        let tick = self.node.clone();
        tokio::task::spawn(async move {
            loop {
                //TODO handle graceful shutdown
                // cleans all data older than 5 minutes
                tick.write().unwrap().tick(5);
                sleep(Duration::from_secs(60)).await;
            }
        });

        let node_warp = warp::any().map(move || self.node.clone());

        let store_value = warp::path!("store")
            .and(warp::body::json())
            .and(node_warp.clone())
            .map(|value: RequestStore, node: Arc<RwLock<Node>>| {
                //TODO update contacts
                node.write().unwrap().store(value.id, value.data);
                warp::reply::with_status("{}", warp::http::StatusCode::CREATED)
            });

        // spawn server
        todo!()
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
        publish(servers, id, value).await;
    }
}

async fn publish(servers: Vec<NodeState>, id: Id, value: Vec<u8>) {
    todo!()
}

async fn try_finding_value(servers: Vec<NodeState>, id: &Id) -> Option<Value> {
    todo!()
}
