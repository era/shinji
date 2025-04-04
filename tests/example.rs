use shinji::{Id, NodeState, Server};

#[tokio::test]
pub async fn setup_two_nodes() {
    let addr_server_1 = "127.0.0.1:8080".parse().unwrap();
    let addr_server_2 = "127.0.0.1:8081".parse().unwrap();
    // We need to setup the servers, we call `new` and `spawn_tasks`
    // `spawn_tasks` will spawn the warp server and the task that cleans
    // expired data from our storage.
    let node_1 = Server::new(addr_server_1, 8080);
    node_1.spawn_tasks().unwrap();
    let node_2 = Server::new(addr_server_2, 8081);
    node_2.spawn_tasks().unwrap();

    // let's make both servers known each other
    node_1.initial_contacts(vec![NodeState::new(addr_server_2)]);
    node_2.initial_contacts(vec![NodeState::new(addr_server_1)]);

    // let's create a new (key, value)
    let id = Id::from("this is a nice key");
    let value = "such a great value".as_bytes().to_vec();

    // let's publish it in the network
    node_1.publish(id.clone(), value.clone()).await;

    // when we publish a value we don't keep it in our own
    // local storage, we only push it to the network
    // so if we try to retrieve it, it should ask node_2 for it
    let result = node_1.get(id).await;

    assert_eq!(Some(value), result);
}
