// This implements encoding logic, this is a trait because encoding logic might work with some
// state
pub trait EncodingLogic<K, V>: Send + Sync {
    fn encode(&mut self, string: K) -> V;
}

// Example Implementation

use std::sync::Arc;

pub struct SimpleLogic {
    // [IMPROVEMENT]:
    // Probably overkill. A u64 should be enough based on the u64::MAX.
    // Does using a u128 instead of a u64 affect performances?
    current_index: u64,
}

impl SimpleLogic {
    pub fn new(base_index: u64) -> Self {
        Self {
            current_index: base_index,
        }
    }
}

impl EncodingLogic<Arc<String>, u64> for SimpleLogic {
    fn encode(&mut self, _string: Arc<String>) -> u64 {
        let res = self.current_index;
        self.current_index += 1;
        res
    }
}
