use crate::encoder::{BiMapTrait, BijectiveMap, EncodingLogic, ParserTrait, Triple};
use std::marker::PhantomData;
use std::path::Path;

type EncodedTriple<T> = (T, T, T);

#[derive(Copy, Clone)]
pub struct EncoderUnit<L, R, E, P, F>
where
    R: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Send + Sync,
    L: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Send + Sync,
    E: EncoderTrait<L, R>,
    P: ParserTrait<L>,
    F: EncodingLogic<L, R>,
{
    left_type: PhantomData<L>,
    right_type: PhantomData<R>,
    parser: P,
    encoding_logic: F,
    bijective_map: Option<E::MapStructure>,
}

impl<L, R, E, P, F> EncoderUnit<L, R, E, P, F>
where
    R: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Send + Sync,
    L: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Send + Sync,
    E: EncoderTrait<L, R>,
    P: ParserTrait<L>,
    F: EncodingLogic<L, R>,
{
    pub fn new(parser: P, encoding_logic: F) -> Self {
        Self {
            left_type: PhantomData,
            right_type: PhantomData,
            parser,
            encoding_logic,
            bijective_map: None,
        }
    }

    pub fn encode<W: AsRef<Path>>(
        &mut self,
        file_path: W,
        index: Option<usize>,
        peers: Option<usize>,
    ) -> E::EncodedDataSet {
        if let None = self.bijective_map {
            let (map, encoded_dataset) = E::load_from_file(
                file_path,
                &mut self.encoding_logic,
                &mut self.parser,
                index,
                peers,
            );
            self.bijective_map = Some(map);
            encoded_dataset
        } else {
            let encoded_dataset = E::insert_from_file(
                file_path,
                &mut self.encoding_logic,
                &mut self.bijective_map.as_mut().unwrap(),
                &mut self.parser,
                index,
                peers,
            )
            // [IMPROVEMENT]:
            // Error handling here please!
            .expect("Could not insert into map");

            encoded_dataset
        }
    }

    pub fn get_map(&self) -> &Option<E::MapStructure> {
        &self.bijective_map
    }

    pub fn get_right_from_map(&mut self, left: L) -> &R {
        match self.bijective_map.as_mut() {
            Some(map) => map.get_right(&left).expect("Could not retrieve right"),
            None => {
                // [IMPROVEMENT]:
                // Add logic maybe to create the map and add the encoding of the left value.
                // This most likely requires the E::MapStructure to implement Default
                // so one can add:
                // self.bijective_map = E::MapStructure::default();
                panic!("Map not initialized");
            }
        }
    }
}

// [DESIGN CHOICE]:
// The trait generalizes the data structure returned by the load function and NOT
// the encoding method. The encoding method is passed as another type to the load function.
// This decouples the two things which are logically different. This allows reusability in
// case one wants to use the same encoding logic for different data structures
// [IMPROVEMENT]:
// Consider making the EncodedDataSet bound by std::iter::Iterator so that you can use
// <EncodedDataSet as std::iter::Iterator>::Item.. but vec does not implement Iterator..
pub trait EncoderTrait<K, V>
where
    V: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
    K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
{
    // Maps each string of type K to another type V
    type MapStructure: BiMapTrait<K, V> + Send + Sync;
    // Set of triples in the encoding domain
    type EncodedDataSet: IntoIterator;

    // The lalrpop parser always returns a vector so it feels safe to "hard code" the type
    // for parsed triple.
    // [PROBLEM]:
    // 1) There's a problem here: the use can give any encoding logic no metter what
    //    the self.map has used before. This might break the bijective property (the effect
    //    is that the element would not be inserted as it would have the value already in the map.
    //    So the contract for this work is to use the same encoding logic used when the map
    //    was created.
    // 2) I can't give an implementation here because the return type is unknown. One way would
    //    be to bind the E::EncodedDataSet to a From<Vec> trait, but meh seems too constraining
    // Possible solutions:
    // 1) Find a way to bind the encoding logic to the encoder (and then to the bijectivemap trait)
    // 2) Make public API use always the same logic.
    // - UPDATE -
    // Same goes for the map parameter. The user MUST supply the same map used before and not a
    // different one.
    // [IMPROVEMENT]:
    // Making a trait for a type to which elements can be put in, we can bind Self::EncodedDataSet
    // to that trait and make this function implemented at trait level
    fn insert_from_parser_output<F, P>(
        map: &mut Self::MapStructure,
        parsed_triples: Vec<P::TripleType>,
        encoding_logic: &mut F,
        index: Option<usize>,
        peers: Option<usize>,
    ) -> Result<Self::EncodedDataSet, (K, V)>
    where
        F: EncodingLogic<K, V>,
        P: ParserTrait<K>;

    // The lalrpop parser will always return a vector of parsed triples, so I am
    // it should be ok not to put a generic parameter for the filename type.
    // [IMPROVEMENT]:
    // As of right now, the load_from_parser_output takes as an input a vector of vector of
    // parsed triples. This is because in the materialization function we need to
    // use the same map for different data sets (t-box and a-box). For this reason each of that
    // dataset will be passed in the vector in input and return separately. This seems weird
    // is there any better solution? This can even require to change the whole structure :/
    // The map contains a Insert function. We can use that. But this works just fine
    fn load_from_parser_output<F, P>(
        parsed_triples: Vec<Vec<P::TripleType>>,
        encoding_logic: &mut F,
        index: Option<usize>,
        peers: Option<usize>,
        // [IMPROVEMENT]:
        // How about defining a structure that has a map and a vec of Encoded data set and return
        // that.
    ) -> (Self::MapStructure, Vec<Self::EncodedDataSet>)
    where
        F: EncodingLogic<K, V>,
        P: ParserTrait<K>;

    fn insert_from_file<F, P, W: AsRef<Path>>(
        file_name: W,
        encoding_logic: &mut F,
        map: &mut Self::MapStructure,
        parser: &mut P,
        index: Option<usize>,
        peers: Option<usize>,
    ) -> Result<Self::EncodedDataSet, (K, V)>
    where
        F: EncodingLogic<K, V>,
        P: ParserTrait<K>,
    {
        let parsed_triples = Self::parse(file_name, parser);
        Self::insert_from_parser_output::<_, P>(map, parsed_triples, encoding_logic, index, peers)
    }

    fn load_from_file<F, P, W: AsRef<Path>>(
        file_name: W,
        encoding_logic: &mut F,
        parser: &mut P,
        index: Option<usize>,
        peers: Option<usize>,
    ) -> (Self::MapStructure, Self::EncodedDataSet)
    where
        F: EncodingLogic<K, V>,
        P: ParserTrait<K>,
    {
        let parsed_triples = vec![Self::parse(file_name, parser)];
        let (map, mut vec) =
            Self::load_from_parser_output::<_, P>(parsed_triples, encoding_logic, index, peers);
        assert_eq!(vec.len(), 1);
        let only_vec = vec
            .pop()
            .expect("THE LOADED RETURNING VEC OF DATASET DID NOT CONTAIN ANY ENCODED DATASET");
        (map, only_vec)
    }

    fn load_from_multiple_files_same_encoded_dataset<F, P, W: AsRef<Path>>(
        file_names: &[W],
        encoding_logic: &mut F,
        parser: &mut P,
        index: Option<usize>,
        peers: Option<usize>,
    ) -> (Self::MapStructure, Self::EncodedDataSet)
    where
        F: EncodingLogic<K, V>,
        P: ParserTrait<K>,
    {
        let mut parsed_triples = vec![];
        for file_name in file_names {
            parsed_triples.append(&mut Self::parse(file_name, parser));
        }
        let (map, mut vec) = Self::load_from_parser_output::<_, P>(
            vec![parsed_triples],
            encoding_logic,
            index,
            peers,
        );
        assert_eq!(vec.len(), 1);
        let only_vec = vec
            .pop()
            .expect("THE LOADED RETURNING VEC OF DATASET DID NOT CONTAIN ANY ENCODED DATASET");
        (map, only_vec)
    }

    fn load_from_multiple_files_different_encoded_dataset<F, P, W: AsRef<Path>>(
        file_names: &[W],
        encoding_logic: &mut F,
        parser: &mut P,
        index: Option<usize>,
        peers: Option<usize>,
    ) -> (Self::MapStructure, Vec<Self::EncodedDataSet>)
    where
        F: EncodingLogic<K, V>,
        P: ParserTrait<K>,
    {
        let mut parsed_triples = vec![];
        for file_name in file_names {
            parsed_triples.push(Self::parse(file_name, parser));
        }
        Self::load_from_parser_output::<_, P>(parsed_triples, encoding_logic, index, peers)
    }

    // [IMPROVEMENT]:
    // The return type seems like it should always be a (String, String, String)
    // because that's what the parser generator returns.
    fn parse<P: ParserTrait<K>, W: AsRef<Path>>(
        file_name: W,
        parser: &mut P,
    ) -> Vec<P::TripleType> {
        // [IMPROVEMENT]:
        // 1) Storing all the data in one big string seems a little meh.
        //    What if the data is REALLY big? We need some sort of caching or reading chunks
        //    from file. Look into this. Use a BufReader
        // 2) Error handling. Instead of returning a Vec<..> throw a result using the ? op.
        //    This require to define my own error and convert all of this errors into that new
        //    one. Turn the expects into a type of error.
        let all_the_data = std::fs::read_to_string(file_name).expect("FAILED TO READ THE FILE");
        parser.parse(&all_the_data[..])
    }

    // [IMPROVEMENT]:
    // While the parser should be using tokio for concurrent parsing of the file, the load funtion
    // does not. Make a parallel load funtion. In our case the parallelism is given by the dataflow
    // using index and peers. For this reasong probably this load_parallel should not be a generic
    // function. But a overwrite
}

// Example Implementation:

use bimap::BiMap;
use std::sync::Arc;

// This specializes encoding data structure
pub struct BiMapEncoder {}

impl EncoderTrait<Arc<String>, u64> for BiMapEncoder {
    type MapStructure = BijectiveMap<Arc<String>, u64>;
    type EncodedDataSet = Vec<EncodedTriple<u64>>;

    fn load_from_parser_output<F, P>(
        parsed_triples: Vec<Vec<P::TripleType>>,
        encoding_fn: &mut F,
        _index: Option<usize>,
        _peers: Option<usize>,
    ) -> (Self::MapStructure, Vec<Self::EncodedDataSet>)
    where
        F: EncodingLogic<Arc<String>, u64>,
        P: ParserTrait<Arc<String>>,
        P::TripleType: Triple<Arc<String>>,
    {
        let mut bimap = BiMap::new();
        let mut resulting_vec = vec![];

        for triples_set in parsed_triples {
            let mut vec = vec![];
            for triple in triples_set {
                // [IMPROVEMENT]:
                // Consider using String interning to retrieve Strings. In this way comparison
                // would be constant with respect to the String length as the references would
                // be compared.
                let (s, p, o) = (triple.s(), triple.p(), triple.o());
                let s_encoded = encoding_fn.encode(s.clone());
                let p_encoded = encoding_fn.encode(p.clone());
                let o_encoded = encoding_fn.encode(o.clone());

                let mut triple = (0, 0, 0);

                // In case of duplicates do not update index.
                if let Ok(()) = bimap.insert_no_overwrite(s.clone(), s_encoded) {
                    triple.0 = s_encoded;
                } else {
                    // In this case is safe to use unwrap() because if we are in the else
                    // branch that means that a value was present.
                    triple.0 = *bimap.get_by_left(&s).unwrap();
                }
                if let Ok(()) = bimap.insert_no_overwrite(p.clone(), p_encoded) {
                    triple.1 = p_encoded;
                } else {
                    triple.1 = *bimap.get_by_left(&p).unwrap();
                }

                if let Ok(()) = bimap.insert_no_overwrite(o.clone(), o_encoded) {
                    triple.2 = o_encoded;
                } else {
                    triple.2 = *bimap.get_by_left(&o).unwrap();
                }

                vec.push(triple);
            }
            resulting_vec.push(vec);
        }
        (BijectiveMap::new(bimap), resulting_vec)
    }

    fn insert_from_parser_output<F, P>(
        map: &mut Self::MapStructure,
        parsed_triples: Vec<P::TripleType>,
        encoding_logic: &mut F,
        _index: Option<usize>,
        _peers: Option<usize>,
    ) -> Result<Self::EncodedDataSet, (Arc<String>, u64)>
    where
        F: EncodingLogic<Arc<String>, u64>,
        P: ParserTrait<Arc<String>>,
    {
        let mut resulting_vec = vec![];
        for triple in parsed_triples {
            // [WARNING]:
            // Is cloning a &Rc the same as cloning the Rc?
            let (s, p, o) = (triple.s(), triple.p(), triple.o());
            let mut triple = (0, 0, 0);

            if let Some(idx) = map.get_right(&s) {
                // if element present in map return its index
                triple.0 = *idx
            } else {
                let s_encoded = encoding_logic.encode(s.clone());
                // Return an error if the string not contained in the map returns an index
                // present in the map.
                map.insert(s.clone(), s_encoded)?;
                triple.0 = s_encoded;
            }
            if let Some(idx) = map.get_right(&p) {
                // if element present in map return its index
                triple.1 = *idx
            } else {
                let p_encoded = encoding_logic.encode(p.clone());
                // Return an error if the string not contained in the map returns an index
                // present in the map.
                map.insert(p.clone(), p_encoded)?;
                triple.1 = p_encoded;
            }
            if let Some(idx) = map.get_right(&o) {
                // if element present in map return its index
                triple.2 = *idx
            } else {
                let o_encoded = encoding_logic.encode(o.clone());
                // Return an error if the string not contained in the map returns an index
                // present in the map.
                map.insert(o.clone(), o_encoded)?;
                triple.2 = o_encoded;
            }

            resulting_vec.push(triple);
        }
        Ok(resulting_vec)
    }
}
