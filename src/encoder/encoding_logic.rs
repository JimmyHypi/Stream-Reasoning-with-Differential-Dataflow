// This implements encoding logic. This trait has no `self` parameter and the encoding function
// should not have any state to favour parallelism. This is just experimental and I don't envision
// to use it. The encoding as of right now it's not parallel. The reason is that for stateless
// logic collisions can cause a big problem.
pub trait StatelessEncodingLogic<K, V>: Send + Sync {
    fn encode(string: K) -> V;
}

// Example Implementation

use std::sync::Arc;

/*pub struct StatelessSimpleLogic {}

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// [IMPROVEMENT]:
// Collisions in this design are very bad. Very.
// Possible solution:
// Let the caller solve collisions. Linear or quadratic probing although this requir check on
// the string.
impl StatelessEncodingLogic<Arc<String>, u64> for StatelessSimpleLogic {
    fn encode(string: Arc<String>) -> u64 {
        let mut hasher = DefaultHasher::new();
        string.hash(&mut hasher);
        hasher.finish()
    }
}
*/

// This implements a stateful simple logic. This is needed for comparison between the stateless
// approach. I want to favor the stateless approach withouth discarding the stateful option
pub trait EncodingLogic<K, V>: Send + Sync {
    fn encode(&mut self, string: K) -> V;
}

// Example Implementation

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
