lalrpop_mod!(pub ntriples);
use std::rc::Rc;

type ParsedTriple<T> = (T, T, T);

// [REQUIRED]:
// This is required because lalrpop does not provide any trait for a parser.. To the best of
// my knowledge.
pub trait ParserTrait<T> {
    fn parse(&mut self, input: &str) -> Vec<ParsedTriple<T>>;
}

pub struct NTriplesParser {
    lalrpop_parser: ntriples::NTriplesStringParser,
}

impl NTriplesParser {
    pub fn new() -> Self {
        let lalrpop_parser = ntriples::NTriplesStringParser::new();
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
