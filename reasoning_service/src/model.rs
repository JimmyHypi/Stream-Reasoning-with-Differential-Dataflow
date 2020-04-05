#![deny(missing_docs)]
//! Model
/// This struct represents an RDF triple
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct Triple {
    pub subject: String,
    pub predicate: String,
    pub object: String,
}

impl Triple {
    /// Prints only the local name with no namespace, just for easy reading
    pub fn print_easy_reading(&self) {
        if let Some(n) = self.subject.as_str().find('#') {
            print!("{} ", &(self.subject.as_str())[n..self.subject.as_str().len()-1]);
        } 
        if let Some(n) = self.predicate.as_str().find('#') {
            print!("{} ", &(self.predicate.as_str())[n..self.predicate.as_str().len()-1]);
        }
        if let Some(n) = self.object.as_str().find('#') {
            println!("{} ", &(self.object.as_str())[n..self.object.as_str().len()-1]);
        } else {
            // in case of labels we have no #
            println!("{} ", self.object);
        }
    }
}

use std::fmt;
impl fmt::Display for Triple{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {} .", self.subject, self.predicate, self.object)
    }
}


/// This struct represents a rule for our specific problem:
/// it has a head, that is a single literal and it has a 
/// body that is two literals
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct CustomRule {
    pub head: CustomLiteral,
    pub body: [CustomLiteral; 2],
}

impl std::fmt::Display for CustomRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "T({}) <= T({}),T({})", self.head, self.body[0], self.body[1])
    }
}

/// This struct represents a literal for our specific problem,
/// so no predicate is needed as the only predicate in our case is the
/// belonging to the data set
/// Our literals will have one predicate and a vector of three terms
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct CustomLiteral{
    // for now I'm going to model all of this with strings
    pub tuple_of_terms: [PossibleTerm; 3],
}

impl std::fmt::Display for CustomLiteral {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}, {}", self.tuple_of_terms[0], self.tuple_of_terms[1], self.tuple_of_terms[2])
    }
}
/// a constant value is an alias for a string for now
pub type ConstantValue = String;
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum PossibleTerm {
    LiteralVariable(String),
    RhoDFProperty(RhoDFWord),
    // We don't have constant values in our case..
    ConstantValue,
}

impl std::fmt::Display for PossibleTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PossibleTerm::LiteralVariable(s) => write!(f, "{}", s),
            PossibleTerm::RhoDFProperty(r) => write!(f, "{}", r),
            PossibleTerm::ConstantValue => write!(f, "{}", self),
        }
    }
}


/// Words in rho-df. Maybe better if they were integer ids.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum RhoDFWord{
    SPO,
    SCO,
    TYPE,
    DOMAIN,
    RANGE,
}

impl std::fmt::Display for RhoDFWord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RhoDFWord::SCO => write!(f, "SCO"),
            RhoDFWord::SPO => write!(f, "SPO"),
            RhoDFWord::TYPE => write!(f, "TYPE"),
            RhoDFWord::DOMAIN => write!(f, "DOMAIN"),
            RhoDFWord::RANGE => write!(f, "RANGE"),
        }
    }
}






