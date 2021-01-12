use crate::EncoderTrait;
use crate::Triple;

type ParsedTriple<T> = (T, T, T);
type DecodedTriples<T> = Vec<ParsedTriple<T>>;

pub trait BiMapTrait<K, V>
where
    V: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
    K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
{
    fn get_right(&self, left: &K) -> Option<&V>;

    fn get_left(&self, right: &V) -> Option<&K>;

    // Insert an element into the bijective map. If element is present return an error.
    // The map is bijective so different K can only map to different V. Remember to create
    // your crate::Error in a module maybe to gather all possible errors. For now I follow the
    // bimap standard to return the attempted pair.
    fn insert(&mut self, left: K, right: V) -> Result<(), (K, V)>;

    // [IMPROVMENT]:
    // So this function returns a Vec<(&K, &K, &K)> should this be generic? Should this return
    // something else?
    fn translate<E>(&self, encoded_dataset: E::EncodedDataSet) -> DecodedTriples<&K>
    where
        E: EncoderTrait<K, V>,
        <E::EncodedDataSet as IntoIterator>::Item: Triple<V>,
    {
        let mut translated = vec![];
        for triple in encoded_dataset {
            let opt_s = self.get_left(triple.s());
            let s_back = if let Some(s) = opt_s {
                s
            } else {
                if let Some(s) = opt_s {
                    s
                } else {
                    // [IMPROVMENT]:
                    // Error Handling!
                    panic!("VALUE NOT PRESENT IN THE MAPS");
                }
            };
            let opt_p = self.get_left(triple.p());
            let p_back = if let Some(p) = opt_p {
                p
            } else {
                if let Some(p) = opt_p {
                    p
                } else {
                    panic!("VALUE NOT PRESENT IN THE MAPS");
                }
            };

            let opt_o = self.get_left(triple.o());
            let o_back = if let Some(o) = opt_o {
                o
            } else {
                if let Some(o) = opt_o {
                    o
                } else {
                    panic!("VALUE NOT PRESENT IN THE MAPS");
                }
            };

            translated.push((s_back, p_back, o_back));
        }
        translated
    }
}

// Example Implementation

use bimap::BiMap;

#[derive(Debug)]
pub struct BijectiveMap<K, V>
where
    V: std::cmp::Eq + std::hash::Hash,
    K: std::cmp::Eq + std::hash::Hash,
{
    bimap: BiMap<K, V>,
}

impl<K, V> BijectiveMap<K, V>
where
    V: std::cmp::Eq + std::hash::Hash,
    K: std::cmp::Eq + std::hash::Hash,
{
    pub fn new(bimap: BiMap<K, V>) -> Self {
        Self { bimap }
    }
}

impl<K, V> BiMapTrait<K, V> for BijectiveMap<K, V>
where
    V: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
    K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
{
    fn get_left(&self, right: &V) -> Option<&K> {
        self.bimap.get_by_right(&right)
    }
    fn get_right(&self, left: &K) -> Option<&V> {
        self.bimap.get_by_left(&left)
    }
    fn insert(&mut self, left: K, right: V) -> Result<(), (K, V)> {
        self.bimap.insert_no_overwrite(left, right)
    }
}
