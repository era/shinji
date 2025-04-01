use crate::id::Id;
use crate::storage::Storage;
use std::collections::VecDeque;
use std::net::SocketAddr;

fn get_bucket_index(distance: &[u8]) -> usize {
    // we need the first non-zero bit (most significant bit)
    for (i, byte) in distance.iter().enumerate() {
        if *byte != 0 {
            return (i * 8) + (8 - byte.leading_zeros() as usize) - 1;
        }
    }
    0
}

pub struct Node {
    routing_table : RoutingTable,
    storage: Storage,
}


impl Node {
    pub fn new(state: NodeState, replication_size: usize, max_routing_table_size: usize) -> Self {
        Self {
            routing_table: RoutingTable::new(state, replication_size, max_routing_table_size),
            storage: Storage {},
        }
    }

    /// either returns the value of the key, or the node which may know about it.
    /// caller should make sure to not keep asking nodes forever in an infinite loop
    pub fn find_value(&self, id: &Id) -> either::Either<Vec<u8>, NodeState> {
        // check if we have seen that Id,
        // if not, return the closest node which may have it.
        todo!()
    }
}

#[derive(Clone)]
pub struct NodeState {
    pub id: Id,
    pub address: SocketAddr,
}

pub struct KBucket {
    nodes: VecDeque<NodeState>,
    size: usize,
}

impl KBucket {
    pub fn new(size: usize) -> Self {
        Self {
            nodes: VecDeque::new(),
            size,
        }
    }

    pub fn update(&mut self, node: NodeState) {
        if let Some(index) = self.nodes.iter().position(|i| i.id == node.id) {
            self.nodes.remove(index);
        } else if self.nodes.len() >= self.size {
            // in theory we should ping the last node
            // and check if it's online
            // for simplicity let's just remove it
            self.nodes.pop_back();
        }

        self.nodes.push_front(node);
    }

    pub fn is_full(&self) -> bool {
        self.nodes.len() == self.size
    }

    pub fn contains(&self, id: &Id) -> bool {
        self.nodes.iter().any(|node| node.id == *id)
    }

    pub fn split(&mut self, id: &Id, index: usize) -> Self {
        let (old_bucket, new_bucket) = self
            .nodes
            .drain(..)
            .partition(|node| get_bucket_index(&node.id.distance(id)) == index);

        let _ = std::mem::replace(&mut self.nodes, old_bucket);

        Self {
            nodes: new_bucket,
            size: self.size,
        }
    }
}

pub struct RoutingTable {
    buckets: Vec<KBucket>,
    state: NodeState,
    max_routing_table_size: usize,
}

impl RoutingTable {
    /// replication_size recommendation: 16
    pub fn new(state: NodeState, replication_size: usize, max_routing_table_size: usize) -> Self {
        Self {
            buckets: vec![KBucket::new(replication_size)],
            state,
            max_routing_table_size,
        }
    }

    // updates the routing table with a new contact, as described in the 2.4 chapter
    // of the paper.
    pub fn update(&mut self, node: NodeState) {
        let distance = get_bucket_index(&self.state.id.distance(&node.id));
        let mut bucket_index = std::cmp::min(distance, self.buckets.len() - 1);

        if self.buckets[bucket_index].contains(&node.id) {
            self.buckets[bucket_index].update(node);
            return;
        }

        loop {
            if !self.buckets[bucket_index].is_full() {
                self.buckets[bucket_index].update(node);
                return;
            }
            let is_last_bucket = bucket_index == self.buckets.len() - 1;
            let is_full = self.buckets.len() == self.max_routing_table_size;

            // If a k-bucket with a different range is full or we cannot split,
            // the new contact is simply dropped
            if !is_last_bucket || is_full {
                return;
            }

            let new_bucket = self.buckets[bucket_index].split(&self.state.id, bucket_index);
            self.buckets.push(new_bucket);

            bucket_index = std::cmp::min(distance, self.buckets.len() - 1);
        }
    }

    // to be used in the FIND_NODE and FIND_VALUE RPC call
    pub fn find_closest_node(&self, id: &Id) -> Option<NodeState> {
        let distance = get_bucket_index(&self.state.id.distance(id));
        let mut bucket_index = std::cmp::min(distance, self.buckets.len() - 1);
        let closest = self.buckets[bucket_index].nodes.iter().min_by_key(|i| i.id.distance(id));
        closest.cloned()
    }

    //TODO find closest_nodes for nodes lookups and STORE RPC call
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_update_new_node() {
        let mut kbucket = KBucket::new(3);
        let id1 = Id([1; 20]);
        let id2 = Id([2; 20]);
        let address1: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let address2: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8081);
        let node1 = NodeState {
            id: id1.clone(),
            address: address1,
        };
        let node2 = NodeState {
            id: id2.clone(),
            address: address2,
        };

        kbucket.update(node1);
        kbucket.update(node2);

        assert_eq!(kbucket.nodes.len(), 2);
        assert_eq!(kbucket.nodes[0].id, id2);
        assert_eq!(kbucket.nodes[1].id, id1);
    }

    #[test]
    fn test_update_existing_node() {
        let mut kbucket = KBucket::new(3);
        let id1 = Id([1; 20]);
        let id2 = Id([2; 20]);
        let address1: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let address2: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8081);
        let node1 = NodeState {
            id: id1.clone(),
            address: address1,
        };
        let node2 = NodeState {
            id: id2.clone(),
            address: address2,
        };

        kbucket.update(node1);
        kbucket.update(node2);
        let node1_updated = NodeState {
            id: id1.clone(),
            address: address1,
        };
        kbucket.update(node1_updated);

        assert_eq!(kbucket.nodes.len(), 2);
        assert_eq!(kbucket.nodes[0].id, id1);
        assert_eq!(kbucket.nodes[1].id, id2);
    }

    #[test]
    fn test_update_exceeding_capacity() {
        let mut kbucket = KBucket::new(2);
        let id1 = Id([1; 20]);
        let id2 = Id([2; 20]);
        let id3 = Id([3; 20]);
        let address1: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let address2: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)), 8081);
        let address3: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)), 8082);
        let node1 = NodeState {
            id: id1.clone(),
            address: address1,
        };
        let node2 = NodeState {
            id: id2.clone(),
            address: address2,
        };
        let node3 = NodeState {
            id: id3.clone(),
            address: address3,
        };

        kbucket.update(node1);
        kbucket.update(node2);
        kbucket.update(node3);

        assert_eq!(kbucket.nodes.len(), 2);
        assert_eq!(kbucket.nodes[0].id, id3);
        assert_eq!(kbucket.nodes[1].id, id2);
    }


    #[test]
    fn test_routing_table_update() {
        let local_id = Id([0; 20]);
        let local_address: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let local_state = NodeState {
            id: local_id.clone(),
            address: local_address,
        };

        let mut routing_table = RoutingTable::new(local_state.clone(), 16, 10);

        let new_node = NodeState {
            id: Id([1; 20]), 
            address: "127.0.0.2:8080".parse().unwrap(),
        };

        routing_table.update(new_node.clone());

        assert!(routing_table.buckets[0].contains(&new_node.id));

        let updated_node = NodeState {
            id: new_node.id.clone(),
            address: "127.0.0.3:8080".parse().unwrap(),
        };

        routing_table.update(updated_node.clone());

        assert!(routing_table.buckets[0].contains(&updated_node.id));
        assert_eq!(routing_table.buckets[0].nodes.front().unwrap().address, updated_node.address);

        for i in 2..17 {
            let node = NodeState {
                id: Id([i; 20]),
                address: format!("127.0.0.{}:8080", i + 1).parse().unwrap(),
            };
            routing_table.update(node);
        }

        assert!(routing_table.buckets[0].is_full());

        let overflow_node = NodeState {
            id: Id([40; 20]),
            address: "127.0.0.18:8080".parse().unwrap(),
        };

        routing_table.update(overflow_node.clone());

        assert_eq!(routing_table.buckets.len(), 2);
        assert!(routing_table.buckets[1].contains(&overflow_node.id));
    }
}
