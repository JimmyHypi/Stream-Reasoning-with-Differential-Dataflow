#![deny(missing_docs)]
//! Model
/// This struct represents an RDF triple
#[derive(Debug)]
pub struct Triple {
    pub subject: String,
    pub property: String,
    pub value: String,
}