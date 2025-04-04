use crate::id::Id;
use crate::node::{Node, NodeState};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::task::JoinSet;
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
/// The main data struct of the library, in order to create a new node
/// on the network you must call `new`.
///
/// ```rust
/// // you should use a public ip instead of local host in real world
/// // the second argument is the port the server should listen to
/// use shinji::Server;
/// let node = Server::new("127.0.0.1:8080".parse().unwrap(), 8080);
/// // spawn all tasks needed such as the server and the storage
/// // maintenance
/// // node.spawn_tasks(); // you need tokio runtime setup to call this
/// // in order to publish a new value to the network you should call
/// // publish
/// // and to get a value from the network you should call
/// // get
/// ```
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
        // using sha1 just because this is a library done only for learning about DHT
        // and sha1 generates hashes with the right size of the key
        // in the original paper
        // this should not be used in a real system due to the risk of collisions.

        let state = NodeState::new(public_ip);

        let node = Node::new(state.clone(), 16, 10);
        let node = Arc::new(RwLock::new(node));

        Self { node, port, state }
    }

    pub fn initial_contacts(&self, contacts: Vec<NodeState>) {
        let mut node = self.node.write().unwrap();
        for contact in contacts {
            node.new_contact(contact);
        }
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
    // the allow for await_holding_lock due to
    // https://github.com/rust-lang/rust-clippy/issues/6446
    #[allow(clippy::await_holding_lock)]
    pub async fn get(&self, id: Id) -> Option<Vec<u8>> {
        let node = self.node.read().unwrap();
        match node.find_value(&id, 5) {
            either::Either::Left(value) => Some(value.data),
            either::Either::Right(servers) => {
                // drop lock, since we don't need to hold it
                // while fetching data from other servers
                drop(node);
                try_finding_value(self.state.clone(), servers, id).await
            }
        }
    }

    /// publish a new (Id, Value) in the network
    // the allow for await_holding_lock due to
    // https://github.com/rust-lang/rust-clippy/issues/6446
    #[allow(clippy::await_holding_lock)]
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
}

/// we could use the response from the servers to keep looking into the value through
/// the whole network. But for simplicity we just look at the servers that were supplied
/// from our own local state.
async fn try_finding_value(sender: NodeState, servers: Vec<NodeState>, id: Id) -> Option<Vec<u8>> {
    let mut set = JoinSet::new();
    let request = RequestFind { sender, id };
    for server in servers {
        let request = request.clone();
        set.spawn(async move {
            let client = reqwest::Client::new();
            client
                .post(format!("http://{}/find_value", server.address))
                .json(&request)
                .send()
                .await
        });
    }
    while let Some(res) = set.join_next().await {
        match res.ok() {
            // in theory it would be better to use the latest seen value
            // for simplicity we don't care
            Some(Ok(res)) if res.status().is_success() => {
                if let Ok(response) = res.json().await {
                    match response {
                        ResponseFind::Value(data, _seen) => return Some(data),

                        _ => tracing::debug!("data was not in the server"),
                    }
                }
            }
            _ => tracing::debug!("ignoring any other response that is not the value itself"),
        };
    }
    None
}
