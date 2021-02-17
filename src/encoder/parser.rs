lalrpop_mod!(pub ntriples);
use crate::encoder::Triple;
use std::sync::Arc;

type ParsedTriple<T> = (T, T, T);

// [REQUIRED]:
// This is required because lalrpop does not provide any trait for a parser.. To the best of
// my knowledge.
pub trait ParserTrait<T>: Send + Sync {
    type TripleType: Triple<T>;

    fn parse_triple(&mut self, input: &str) -> Self::TripleType;
}

pub struct NTriplesParser {
    lalrpop_parser: ntriples::ArcStringTripleParser,
}

impl NTriplesParser {
    pub fn new() -> Self {
        let lalrpop_parser = ntriples::ArcStringTripleParser::new();
        Self { lalrpop_parser }
    }
}

impl ParserTrait<Arc<String>> for NTriplesParser {
    type TripleType = ParsedTriple<Arc<String>>;

    fn parse_triple(&mut self, input: &str) -> Self::TripleType {
        self.lalrpop_parser
            .parse(input)
            .expect(format!("FAILED TO PARSE TRIPLE: {}", input).as_str())
    }
}
