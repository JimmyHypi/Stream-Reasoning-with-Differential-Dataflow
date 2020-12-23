lalrpop_mod!(pub ntriples);
use bimap::BiMap;
use ntriples::NTriplesStringParser;
use std::rc::Rc;

// [IMPROVEMENT]:
// 1) This type alias represents the type returned by the triple. As of right now it
//    returns a reference count to a String. Consider changing it to maybe a Rc<str>.
// 2) As of right now I'm forcing the parsed triple to be a tuple. This should probably
//    be generic. Allowing other posisble representation of a triple (e.g. &[T, 3]) or a
//    user defined struct
type ParsedTriple<T> = (T, T, T);
pub type EncodedTriple<T> = (T, T, T);

// [REQUIRED]:
// This is required because lalrpop does not provide any trait for a parser.. To the best of
// my knowledge.
pub trait ParserTrait<T> {
    fn parse(&mut self, input: &str) -> Vec<ParsedTriple<T>>;
}

struct NTriplesParser {
    lalrpop_parser: NTriplesStringParser,
}

impl NTriplesParser {
    pub fn new(lalrpop_parser: NTriplesStringParser) -> Self {
        Self { lalrpop_parser }
    }
}

impl ParserTrait<Rc<String>> for NTriplesParser {
    fn parse(&mut self, input: &str) -> Vec<ParsedTriple<Rc<String>>> {
        self.lalrpop_parser
            .parse(input)
            .expect("FAILED TO PARSE STRING")
    }
}

pub trait BiMapTrait<K, V>
where
    V: std::cmp::Eq + std::hash::Hash,
    K: std::cmp::Eq + std::hash::Hash,
{
    fn get_right(&self, left: &K) -> Option<&V>;
    fn get_left(&self, right: &V) -> Option<&K>;
}

// [DESIGN CHOICE]:
// The trait generalizes the data structure returned by the load function and NOT
// the encoding method. The encoding method is passed as another type to the load function.
// This decouples the two things which are logically different. This allows reusability in
// case one wants to use the same encoding logic for different data structures
pub trait Encoder<K, V>
where
    V: std::cmp::Eq + std::hash::Hash,
    K: std::cmp::Eq + std::hash::Hash,
{
    // Maps each string of type K to another type V
    type MapStructure: BiMapTrait<K, V>;
    // Set of triples in the encoding domain
    type EncodedDataSet;
    fn load<F, P>(
        file_name: &str,
        encoding_logic: &mut F,
        parser: &mut P,
        index: Option<usize>,
        peers: Option<usize>,
    ) -> (Self::MapStructure, Self::EncodedDataSet)
    where
        F: EncodingLogic<K, V>,
        P: ParserTrait<K>;

    // [IMPROVEMENT]:
    // The return type seems like it should always be a (String, String, String)
    // because that's what the parser generator returns.
    fn parse<P: ParserTrait<K>>(file_name: &str, parser: &mut P) -> Vec<ParsedTriple<K>> {
        // [IMPROVEMENT]:
        // 1) Storing all the data in one big string seems a little meh.
        //    What if the data is REALLY big? We need some sort of caching or reading chunks
        //    from file. Look into this.
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

// This implements encoding logic, this is a trait because encoding logic might work with some
// state
pub trait EncodingLogic<K, V> {
    fn encode(&mut self, string: K) -> V;
}

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

impl EncodingLogic<Rc<String>, u64> for SimpleLogic {
    fn encode(&mut self, _string: Rc<String>) -> u64 {
        let res = self.current_index;
        self.current_index += 1;
        res
    }
}

// This specializes encoding data structure
pub struct BiMapEncoder {}

pub struct BijectiveMap<K, V>
where
    V: std::cmp::Eq + std::hash::Hash,
    K: std::cmp::Eq + std::hash::Hash,
{
    bimap: BiMap<K, V>,
}
impl<K, V> BiMapTrait<K, V> for BijectiveMap<K, V>
where
    V: std::cmp::Eq + std::hash::Hash,
    K: std::cmp::Eq + std::hash::Hash,
{
    fn get_left(&self, right: &V) -> Option<&K> {
        self.bimap.get_by_right(&right)
    }
    fn get_right(&self, left: &K) -> Option<&V> {
        self.bimap.get_by_left(&left)
    }
}

impl Encoder<Rc<String>, u64> for BiMapEncoder {
    type MapStructure = BijectiveMap<Rc<String>, u64>;
    type EncodedDataSet = Vec<EncodedTriple<u64>>;
    fn load<F, P>(
        file_name: &str,
        mut encoding_fn: &mut F,
        parser: &mut P,
        _index: Option<usize>,
        _peers: Option<usize>,
    ) -> (Self::MapStructure, Self::EncodedDataSet)
    where
        F: EncodingLogic<Rc<String>, u64>,
        P: ParserTrait<Rc<String>>,
    {
        let mut bimap = BiMap::new();
        let mut vec = vec![];
        // [IMPROVEMENT]:
        // It seems a little ridiculous how the load function has the parser and it calls another
        // function while it could do everything by itself.. but hey, goal separation
        let triples = Self::parse(file_name, parser);

        for triple in triples {
            // [IMPROVEMENT]:
            // Consider using String interning to retrieve Strings. In this way comparison
            // would be constant with respect to the String length as the references would
            // be compared.
            let (s, p, o) = triple;
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
        (BijectiveMap { bimap }, vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn parser_test() {
        log_init();
        let vector = NTriplesStringParser::new().parse(
            "<http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Document> .
        <http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://purl.org/dc/terms/title> \"N-Triples\"@en-US .
        <http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://xmlns.com/foaf/0.1/maker> _:art .
        <http://www.w3.org/2001/sw/RDFCore/ntriples/> <http://xmlns.com/foaf/0.1/maker> _:dave .
        _:art <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
        _:art <http://xmlns.com/foaf/0.1/name> \"Art Barstow\"@en-US .
        _?x <http://xmlns.com/foaf/0.1/name> \"Art Barstow\"@en-US .
        _:dave <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Person> .
        _:dave <http://xmlns.com/foaf/0.1/name> \"Dave Beckett\"^^xsd::string .",
               );
        assert!(vector.is_ok());

        let vector = vector.unwrap();

        assert_eq!(9, vector.len());
    }

    use std::fs;

    #[test]
    fn parser_test_file() {
        log_init();
        let filename = "data/univ-bench-preprocessed.nt";
        let string = fs::read_to_string(filename).expect("Error while reading file");
        let parsed = NTriplesStringParser::new().parse(&string);
        assert!(parsed.is_ok());

        let parsed = parsed.unwrap();

        assert_eq!(364, parsed.len());
    }

    #[test]
    fn encoder_test() {
        log_init();
        let parser = NTriplesParser::new(NTriplesStringParser::new());
        let logic = SimpleLogic::new(0);
        let bimap = BiMapEncoder::load(
            "data/test_for_simple_reasoning.nt",
            logic,
            parser,
            None,
            None,
        );
        let (dictionary, encoded_dataset) = bimap;

        assert_eq!(3, encoded_dataset.len());
        assert_eq!(7, dictionary.bimap.len());
    }
}
