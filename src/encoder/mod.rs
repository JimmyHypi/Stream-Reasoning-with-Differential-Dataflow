lalrpop_mod!(pub ntriples);
use bimap::BiMap;
use log::{debug, error, info, log_enabled, Level};
use ntriples::NTriplesStringParser;
use std::rc::Rc;

// [IMPROVEMENT]:
// 1) This type alias represents the type returned by the triple. As of right now it
//    returns a reference count to a String. Consider changing it to maybe a Rc<str>.
// 2) As of right now I'm forcing the parsed triple to be a tuple. This should probably
//    be generic. Allowing other posisble representation of a triple (e.g. &[T, 3]) or a
//    user defined struct
type ParsedTriple<T> = (T, T, T);

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

// [DESIGN CHOICE]:
// The trait generalizes the data structure returned by the load function and NOT
// the encoding method. The encoding method is passed as a closure to the load function.
// This decouples the two things which are logically different.
pub trait Encoder<K, V>
where
    V: std::cmp::Eq + std::hash::Hash,
{
    type Output;
    fn load<F, P>(file_name: &str, encoding_logic: F, parser: P) -> Self::Output
    where
        F: EncodingLogic<K, V>,
        P: ParserTrait<K>;
    // [IMPROVEMENT]:
    // The return type seems like it should always be a (String, String, String)
    // because that's what the parser generator returns.
    fn parse<P: ParserTrait<K>>(file_name: &str, mut parser: P) -> Vec<ParsedTriple<K>> {
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
    current_index: u128,
}

impl EncodingLogic<Rc<String>, u128> for SimpleLogic {
    fn encode(&mut self, _string: Rc<String>) -> u128 {
        let res = self.current_index;
        self.current_index += 1;
        res
    }
}

// This specializes encoding data structure
pub struct BiMapEncoder {}

impl Encoder<Rc<String>, u128> for BiMapEncoder {
    // [IMPROVEMENT]:
    // Should this be a weak or a strong reference count.
    type Output = BiMap<Rc<String>, u128>;
    fn load<F, P>(file_name: &str, mut encoding_fn: F, parser: P) -> Self::Output
    where
        F: EncodingLogic<Rc<String>, u128>,
        P: ParserTrait<Rc<String>>,
    {
        let mut bimap = BiMap::new();
        // [IMPROVEMENT]:
        // It seems a little ridiculous how the load function has the parser and it calls another
        // function while it could do everything by itself..
        let triples = Self::parse(file_name, parser);
        for triple in triples {
            // [IMPROVEMENT]:
            // Consider using String interning to retrieve Strings. In this way comparison
            // would be constant with respect to the String length as the references would
            // be compared.
            let (s, p, o) = triple;
            // In case of duplicates do not update index.
            bimap
                .insert_no_overwrite(s.clone(), encoding_fn.encode(s.clone()))
                .expect("ERROR ACCESSING BIMAP");
            bimap
                .insert_no_overwrite(p.clone(), encoding_fn.encode(p.clone()))
                .expect("ERROR ACCESSING BIMAP");
            bimap
                .insert_no_overwrite(o.clone(), encoding_fn.encode(o.clone()))
                .expect("ERROR ACCESSING BIMAP");

            assert_eq!(Rc::strong_count(&s), 3);
        }
        bimap
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
}
