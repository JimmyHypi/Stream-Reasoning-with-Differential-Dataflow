#![deny(missing_docs)]
//! Functions and definitions
pub mod model;

use rio_xml::{RdfXmlParser, RdfXmlError};
use rio_api::parser::TriplesParser;
use std::io::BufReader;
use std::fs::File;


/// Assumptions: 
///     - No repeated value in the A_Box
///     - Data uses only ASCII characters
/// function to load the data, parallelized with respect to the number of workers.
pub fn load_data(filename: &str, index: usize, peers: usize) -> Vec<model::Triple>{
    use std::io::BufRead;

    let mut returning_data = Vec::new();
    let file = BufReader::new(File::open(filename).expect("Couldn't open file"));
    let triples = file.lines();
    
    // We use enumerate to parallelize the loading of the data
    for (count, read_triple) in triples.enumerate() {
        if index == count % peers {
            if let Ok(triple) = read_triple {
                let v: Vec<String> = triple.split(" ")
                      .map(|x| String::from(x))
                      .collect();
                let triple_to_push = 
                    model::Triple{
                        // TODO: THESE CLONES, MEH
                        subject: v[0].clone(),
                        predicate: v[1].clone(),
                        object: v[2].clone()
                    };
                returning_data.push(triple_to_push);
            } else {
                // TODO
            }
        }
    }

    returning_data
}


use std::collections::HashSet;
/// loads the ontology which has already been preprocessed using apache Jena
/// the ASSUMPTION that I'm making here is that the ontology will never be
/// to large. It returns a set so that we will not consider duplicate that 
/// apache Jena generates when reasoning
pub fn load_ontology(filename: &str) -> HashSet<crate::model::Triple>{
    
    let file = File::open(filename).expect("couldn't open file");
    let reader = BufReader::new(file);
    let mut res: HashSet<crate::model::Triple> = HashSet::new();
    // Here I'm using the parser already present in crate.io, so I can really 
    // parallelize the reading, and I don't even think it is worth it,
    // because (even in the paper) we assume that schema type triples are
    // always going to be in a small number, which imho makes sense,
    // as it is a TBOX
    RdfXmlParser::new(reader, "").unwrap().parse_all(&mut |t| {
        // IMPORTANT: THIS CHECK IS TO AVOID CONSIDERING THE FACT THAT 
        // A CLASS OR PROPERTY IS SUBCLASSOF OR SUBPROPERTYOF ITSELF
        if t.subject.to_string() != t.object.to_string() {
            res.insert(
                crate::model::Triple{
                    // Clones sucks, but is this going to be a bottleneck?
                    // At the end of the day, this is going to be performed 
                    // n times, where n is the number of triples in the ontology
                    subject: t.subject.to_string().clone(),
                    predicate: t.predicate.to_string().clone(),
                    object: t.object.to_string().clone(),
                }
            );
        }
        Ok(()) as Result<(), RdfXmlError>
    }).expect("something wrong with the parser");

    res
}

/// Assumption: The load rules function works only for the ruleset
/// we expect in our application 
/// parses the rules and returs them as a Vector
pub fn load_rules(filename: &str) -> Vec::<model::CustomRule> {
    use std::io::BufRead;

    let mut returning_data = Vec::new();
    let file = BufReader::new(File::open(filename).expect("Couldn't open file"));
    let rules = file.lines();

    for rule in rules {
        if let Ok(r) = rule {
            let cut_off = r.find(":").expect("Could not find the cut off between head and rules: check the syntax of the rules");
            let head_substring = String::from(&r[.. cut_off-1]);
            let body_substring = String::from(&r[cut_off+3 ..]);
            
            // println!("\n\nRule: {}\nHead: {}\nBody: {}\n", r, head_substring, body_substring);
            let body_literals_as_str: Vec::<&str> = body_substring.split(',').collect();
            // for (count, literal) in body_literals_as_str.iter().enumerate() {
            //     println!("BodyLiteral {}: {}", count, literal);
            // }
            let parameters_list = String::from(&head_substring[1..head_substring.len()-1]);
            let head_terms: Vec<&str> = parameters_list.split_whitespace().collect();
            let head_literal = build_literal(head_terms);

            let parameters_list1 = String::from(&(body_literals_as_str[0])[1..body_literals_as_str[0].len()-1]);
            let literal_terms1: Vec<&str> = parameters_list1.split_whitespace().collect();
            let parameters_list2 = String::from(&(body_literals_as_str[1])[1..body_literals_as_str[1].len()-1]); 
            let literal_terms2: Vec<&str> = parameters_list2.split_whitespace().collect();
            let body_literals: [model::CustomLiteral; 2] = [build_literal(literal_terms1), build_literal(literal_terms2)];

            let rule_to_be_pushed = build_rule(head_literal, body_literals);
            returning_data.push(rule_to_be_pushed);
        } else {
            // TODO
        }
    }


    returning_data
}


// Private Functions
fn build_literal(params: Vec::<&str>) -> model::CustomLiteral {
    
    let first_param: model::PossibleTerm = model::PossibleTerm::LiteralVariable(String::from(&(params[0])[1..]));
    let second_param: model::PossibleTerm = {
        if "?" == &(params[1])[0..1] {
            // In this case it is a variable
            let var_name = &(params[1])[1..];
            model::PossibleTerm::LiteralVariable(String::from(var_name))
        } else {
            // In this case it is a word in RhoDF
            match params[1] {
                "SCO"    => model::PossibleTerm::RhoDFProperty(model::RhoDFWord::SCO),
                "SPO"    => model::PossibleTerm::RhoDFProperty(model::RhoDFWord::SPO),
                "TYPE"   => model::PossibleTerm::RhoDFProperty(model::RhoDFWord::TYPE),
                "DOMAIN" => model::PossibleTerm::RhoDFProperty(model::RhoDFWord::DOMAIN),
                "RANGE"  => model::PossibleTerm::RhoDFProperty(model::RhoDFWord::RANGE),
                _ => {
                    // In all other cases we have constant values but in our rules
                    // we have no constant values so let's just panic for now
                    panic!("malformed ruleset");
                }
            }
        }
    };
    let third_param: model::PossibleTerm = model::PossibleTerm::LiteralVariable(String::from(&(params[2])[1..]));

    model::CustomLiteral{
        tuple_of_terms: [first_param, second_param, third_param],
    }
}
// Only works for rules with one literal in the head
fn build_rule(head_literal: model::CustomLiteral, body_literals: [model::CustomLiteral; 2]) -> model::CustomRule {
    model::CustomRule{
        head: head_literal,
        body: body_literals,
    }
}

// /// Just to see how it goes let's start with evaluating rule number one, if
// /// we evaluate all the rules separately we don't need to parse the rules .-.
// pub fn evaluate_first_rule(data: &'static Vec<model::Triple>) -> Vec::<model::Triple> {
//     use timely::dataflow::operators::ToStream;
//     use timely::dataflow::operators::Map;
//     use differential_dataflow::AsCollection;
//     // The first rule consists in: T(x TYPE b) <= T(a, SCO, b),T(x, TYPE, a)
//     // to do that we want to use differential dataflow:
//     timely::execute_from_args(std::env::args(), move |worker| {

//         worker.dataflow::<usize,_,_>(|scope| {
//             data
//                 .to_stream(scope)
//                 .map(|triple| (triple, 0, 1))
//                 .as_collection()
//                 .inspect(|x| println!("Saw: {:?}", x))
//                 ;
//         })

//     }).expect("Worker didn't exit properly");

//     vec![]

// }       


#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

}
