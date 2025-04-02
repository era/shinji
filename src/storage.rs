use crate::id::Id;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::Instant;
use time::{Duration, OffsetDateTime};

#[derive(Clone)]
pub struct Value {
    pub data: Vec<u8>,
    seen: Instant,
    pub timestamp: i64,
}

impl Value {
    fn new(data: Vec<u8>) -> Self {
        let now = OffsetDateTime::now_utc();
        let timestamp = now.unix_timestamp();
        Self {
            data,
            seen: Instant::now(),
            timestamp,
        }
    }
}

#[derive(Default)]
pub struct Storage {
    data: HashMap<Id, Value>,
    seen: BTreeMap<Instant, HashSet<Id>>,
}

impl Storage {
    pub fn put(&mut self, id: Id, value: Vec<u8>) {
        let now = Instant::now();

        if let Some(old_entry) = self.data.insert(id.clone(), Value::new(value)) {
            // if we have this value already, we need to update it in seen as well
            // so first we remove the old one
            if let Some(seen) = self.seen.get_mut(&old_entry.seen) {
                seen.remove(&id);
            }
        }

        self.seen.entry(now).or_insert_with(HashSet::new).insert(id);
    }
    pub fn get(&self, id: &Id) -> Option<Value> {
        self.data.get(id).cloned()
    }

    pub fn clean(&mut self, expiration_minutes: i64) {
        let expiration_cutoff = Instant::now() - Duration::minutes(expiration_minutes);
        let cuttof = self.seen.split_off(&expiration_cutoff);
        for id in cuttof.into_iter().flat_map(|(_, e)| e.into_iter()) {
            self.data.remove(&id);
        }
    }
}
