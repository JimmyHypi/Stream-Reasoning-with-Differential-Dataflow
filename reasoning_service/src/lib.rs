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
/// At this point of the work multiple workers are not considered because the logic to shuffle the data
/// is not trivial at all
pub fn load_data(filename: &str, index: usize, peers: usize) -> Vec<model::Triple> {
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

use timely::dataflow::Scope;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;

/// First rule: T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)
pub fn rule_1<G>(
    data_collection: &Collection<G, model::Triple>,
) -> Collection<G, model::Triple>
where
    G: Scope, 
    G::Timestamp: Lattice,
{
    let sco_transitive_closure =        
        data_collection
            .filter(|triple| triple.predicate == model::RDFS_SUB_CLASS_OF)
            .iterate(|inner| {
                
                inner 
                    .map(|triple| (triple.object, (triple.subject, triple.predicate)))
                    .join(&inner.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                    .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))| 
                        model::Triple {
                            subject: subj1,
                            predicate: pred1,
                            object: obj2,
                        }
                    )
                    .concat(&inner)
                    .threshold(|_,c| { if c > &0 { 1 } else if c < &0 { -1 } else { 0 } })
                    
            })
            //.inspect(|x| println!("AFTER_RULE_1: {:?}", x))

        ;

    sco_transitive_closure
                    
}


/// Second rule: T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)
pub fn rule_2<G>(
    data_collection: &Collection<G, model::Triple>,
) -> Collection<G, model::Triple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let spo_transitive_closure = 
        data_collection
            .filter(|triple| triple.predicate == model::RDFS_SUB_PROPERTY_OF)
            .iterate(|inner| {
                                
                inner 
                    .map(|triple| (triple.object, (triple.subject, triple.predicate)))
                    .join(&inner.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                    .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))| 
                        model::Triple {
                            subject: subj1,
                            predicate: pred1,
                            object: obj2,
                        }
                    )
                    .concat(&inner)
                    .threshold(|_,c| { if c > &0 { 1 } else if c < &0 { -1 } else { 0 } })
                
            })
            ;

    spo_transitive_closure
}

/// Third rule: T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)
pub fn rule_3<G>(
    data_collection: &Collection<G, model::Triple>,
) -> Collection<G, model::Triple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let sco_only = 
        data_collection.filter(|triple| triple.predicate == model::RDFS_SUB_CLASS_OF)
        ;

    let candidates =
        data_collection
            .filter(|triple| triple.predicate == model::RDF_TYPE)
            .map(|triple| (triple.object.clone(), (triple)))
            .join(&sco_only.map(|triple| (triple.subject, ())))
            .map(|(_key, (triple, ()))| triple)
            ;

    let sco_type_rule = 
        candidates
            .iterate(|inner| {
                let sco_only_in = 
                    sco_only
                        .enter(&inner.scope())
                        ;

                inner
                    .map(|triple| (triple.object, (triple.subject, triple.predicate)))
                    .join(&sco_only_in.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                    .map(|(_key, ((x, typ), (_sco, b)))| 
                        model::Triple {
                            subject: x,
                            predicate: typ,
                            object: b
                        }
                    )
                    .concat(&inner)
                    .threshold(|_,c| { if c > &0 { 1 } else if c < &0 { -1 } else { 0 } })
            })
            ;

    sco_type_rule
}

/// Fourth rule: T(x, p, b) <= T(p1, SPO, p),T(x, p1, y)
pub fn rule_4<G>(
    data_collection: &Collection<G, model::Triple>,
) -> Collection<G, model::Triple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    // Select only the triples whose predicate participates in a SPO triple
    let spo_only_out =
        data_collection
            .filter(|triple| triple.predicate == model::RDFS_SUB_PROPERTY_OF)
        ;
    
    let candidates = 
        data_collection
            .map(|triple| ((triple.predicate.clone()),triple))
            .join(&spo_only_out.map(|triple| ((triple.subject),())))
            .map(|(_, (triple, ()))| triple)
            ; 
    
    let spo_type_rule = 
        candidates
            .iterate(|inner| {
                let spo_only = 
                    spo_only_out
                        .enter(&inner.scope())
                        ;
                inner
                    .map(|triple| (triple.predicate, (triple.subject, triple.object)))
                    .join(&spo_only.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                    .map(|(_key, ((x, y), (_spo, p)))| 
                        model::Triple {
                            subject: x,
                            predicate: p,
                            object: y,
                        }
                    )
                    .concat(&inner)
                    .threshold(|_,c| { if c > &0 { 1 } else if c < &0 { -1 } else { 0 } })
            })
            ;
    spo_type_rule
}

/// Fifth rule: T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)
pub fn rule_5<G>(
    data_collection: &Collection<G, model::Triple>,
) -> Collection<G, model::Triple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let only_domain =
        data_collection
            .filter(|triple| triple.predicate == model::RDFS_DOMAIN)
            ;

    let candidates = 
        data_collection
            .map(|triple| ((triple.predicate.clone()),triple))
            .join(&only_domain.map(|triple| (triple.subject, ())))
            .map(|(_, (triple, ()))| triple)
            ; 

    // This does not require a iterative dataflow, the rule does not produce
    // terms that are used by the rule itself
    let domain_type_rule =
        candidates
            .map(|triple| (triple.predicate, (triple.subject, triple.object)))
            .join(&only_domain.map(|triple| (triple.subject, (triple.predicate, triple.object))))
            .map(|(_key, ((a, _b), (_dom, d)))| 
                model::Triple {
                    subject: a,
                    predicate: String::from(model::RDF_TYPE),
                    object: d,
                }
            )
            ;

    domain_type_rule
}

/// Sixth rule: T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)
pub fn rule_6<G>(
    data_collection: &Collection<G, model::Triple>,
) -> Collection<G, model::Triple>
where
    G: Scope,
    G::Timestamp: Lattice,
{
    let only_range =
        data_collection
            .filter(|triple| triple.predicate == model::RDFS_RANGE)
            ;

    let candidates = 
        data_collection
            .map(|triple| ((triple.predicate.clone()),triple))
            .join(&only_range.map(|triple| (triple.subject, ())))
            .map(|(_, (triple, ()))| triple)
            ; 

    // This does not require a iterative dataflow, the rule does not produce
    // terms that are used by the rule itself
    let domain_type_rule =
        candidates
            .map(|triple| (triple.predicate, (triple.subject, triple.object)))
            .join(&only_range.map(|triple| (triple.subject, (triple.predicate, triple.object))))
            .map(|(_key, ((_a, b), (_ran, r)))| 
                model::Triple {
                    subject: b,
                    predicate: String::from(model::RDF_TYPE),
                    object: r,
                }
            )
            ;

    domain_type_rule
}


use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::probe::Probe;
use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;

/// Computes the full materialization of the collection 
pub fn full_materialization<G>(
    data_input: &Collection<G, model::Triple>,
    mut probe: &mut ProbeHandle<G::Timestamp>,
) -> TraceAgent<OrdKeySpine<model::Triple, G::Timestamp, isize>>
where 
    G: Scope,
    G::Timestamp: Lattice
{
    // ASSUMPTION: WE ARE HARDCODING THE RULES IN HERE
            // We only have two kinds of rules:
            // the ones that deal with only the T_box:
            // T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)
            // T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)
            // the ones that deal with both the a_box and the t_box
            // T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)
            // T(x, p, y) <= T(p1, SPO, p),T(x, p1, y)
            // T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)
            // T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)



            // Orders matters, to guarantee a correct execution of the materialization:
            // T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)        -- rule_1
            // T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)        -- rule_2
            // T(x, p, y) <= T(p1, SPO, p),T(x, p1, y)          -- rule_4
            // T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)      -- rule_5
            // T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)       -- rule_6
            // T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)      -- rule_3
            // as we can see there is no rule with a literal in the body that
            // corresponds to a literal in the head of any subsequent rule
            
            
            let sco_transitive_closure = rule_1(&data_input);

            let spo_transitive_closure = rule_2(&data_input);

            let data_input = data_input
                                .concat(&sco_transitive_closure)
                                .concat(&spo_transitive_closure)
                                //  VERY IMPORTANT: THE DISTINCT PUTS THE REMOVAL INTO ADDITION
                                // SO WE REWRITE THE DISTINCT TO KEEP THE REMOVAL -1
                                // .distinct()
                                .threshold(|_,c| {
                                    if c > &0 { 1 } else if c < &0 { -1 } else { 0 }
                                 })
                                ;
            

            let spo_type_rule = rule_4(&data_input);
            
            let data_input = data_input
                                .concat(&spo_type_rule)
                                // .distinct()
                                .threshold(|_,c| {
                                    if c > &0 { 1 } else if c < &0 { -1 } else { 0 }
                                 })
                                ;

            let domain_type_rule = rule_5(&data_input);

            // We don't need this, but still :P
            let data_input = data_input
                                .concat(&domain_type_rule)
                                // .distinct()
                                .threshold(|_,c| {
                                    if c > &0 { 1 } else if c < &0 { -1 } else { 0 }
                                 })
                                ;

            let range_type_rule = rule_6(&data_input);

            let data_input = data_input
                                .concat(&range_type_rule)
                                // .distinct()
                                .threshold(|_,c| {
                                    if c > &0 { 1 } else if c < &0 { -1 } else { 0 }
                                 })
                                ;

            let sco_type_rule = rule_3(&data_input);

            let data_input = data_input
                                .concat(&sco_type_rule)
                                // .distinct()
                                .threshold(|_,c| {
                                    if c > &0 { 1 } else if c < &0 { -1 } else { 0 }
                                 })
                                ;
                
            let arrangement = 
                data_input
                    // .inspect(|triple| (triple.0).print_easy_reading())
                    // .inspect(|triple| println!("{:?}", triple))
                    // .inspect(|x| println!("{:?}", x))
                    .arrange_by_self()
                    ;

            arrangement
                .stream
                .probe_with(&mut probe)
                ;
            
            arrangement.trace
}

use differential_dataflow::input::InputSession;
/// Sets up the data for to use for the full materialization
/// Here some preprocessing is required, we hav to eliminate all the owl tags and change the "terminological type" to distinguish it from the axiomatic one
pub fn load_lubm_data(
    a_box_filename: &str,
    t_box_filename: &str,
    index: usize,
    peers: usize
) -> (Vec<model::Triple>, Vec<model::Triple>) {
    // let a_box = reasoning_service::load_data(&format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\LUB1_nt_consolidated\\Universities.nt"), index, peers);
    let a_box = load_data(a_box_filename, index, peers);
    // let t_box = reasoning_service::load_ontology("C:\\Users\\xhimi\\Documents\\University\\THESIS\\univ-bench-prefix-changed.owl");
    let t_box = load_ontology(t_box_filename);
    // Preprocess the terminological box
    let t_box = preprocess(t_box);
    (t_box, a_box)
}

fn preprocess(t_box: HashSet<model::Triple>) -> Vec<model::Triple> {
    let mut res: Vec<model::Triple> = Vec::new();
    // TODO: Preprocess the data restricting only triples in rdfs (RhoDF)
    for triple in t_box {
        res.push(triple)
    }
    res
} 


/// insert data provided by the abox and tbox into the dataflow through
/// the input handles. Here all the 
pub fn insert_starting_data(
    a_box: Vec<model::Triple>,
    data_input: &mut InputSession<usize, model::Triple, isize>,
    t_box: Vec<model::Triple>,
) {

    
    for triple in t_box {
        data_input.insert(triple);
    }
    for triple in a_box {
        data_input.insert(triple);
    }

    // initial data are inserted all with timestamp 0, so we advance at time 1 and schedule the worker
    data_input.advance_to(1); data_input.flush();
}

/// Incremental addition maintenance
pub fn add_data(
    a_box_batch: Vec<model::Triple>,
    data_input: &mut InputSession<usize, model::Triple, isize>,
    t_box_batch: Vec<model::Triple>,
    time_to_advance_to: usize
) {
    for triple in t_box_batch {
        data_input.insert(triple);
    }

    for triple in a_box_batch {
        data_input.insert(triple);
    }

    data_input.advance_to(time_to_advance_to); data_input.flush();
}

// ASSUMPTION: closed world: if `something sco something_else` is missing it means that it is not true that `something sco something_else`
/// Incremental deletion maintenance:
pub fn remove_data(
    a_box_batch: Vec<model::Triple>,
    data_input: &mut InputSession<usize, model::Triple, isize>,
    t_box_batch: Vec<model::Triple>,
    time_to_advance_to: usize
) {

    for triple in t_box_batch {
        data_input.remove(triple);
    }

    for triple in a_box_batch {
        data_input.remove(triple);
    }


    data_input.advance_to(time_to_advance_to); data_input.flush();
}

/// Save the full materialization fo file
pub fn save_to_file_through_trace(
    path: &str, 
    trace: &mut TraceAgent<OrdKeySpine<model::Triple, usize, isize>>,
    time: usize
) 
{  
    use std::fs::OpenOptions;
    use std::io::Write;
    use differential_dataflow::trace::TraceReader;
    use differential_dataflow::trace::cursor::Cursor;

    // let mut full_materialization_file = OpenOptions::new()
    //     .read(true)
    //     .write(true)
    //     .append(true)
    //     .create(true)
    //     .open(path)
    //     .expect("Something wrong happened with the ouput file");

    if let Some((mut cursor, storage)) = trace.cursor_through(&[time]){
        while let Some(key) = cursor.get_key(&storage) {
            while let Some(&()) = cursor.get_val(&storage) {
                let mut count = 0;
                use timely::order::PartialOrder;
                cursor.map_times(&storage, |t, diff| {
                    // println!("{}, DIFF:{:?} ", key, diff);
                    if t.less_equal(&(time-1)) {
                        count += diff;
                    }
                });
                if count > 0 {
                    // println!("{}", key);
                    key.print_easy_reading();
                    // if let Err(e) = writeln!(full_materialization_file, "{}", key.to_string()) {
                    //     eprintln!("Couldn't write to file: {}", e);
                    // }
                } 
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage);
        }
    } else {
        println!("COULDN'T GET CURSOR");
    }

}


/// Saves the fragment of the materialization related to a worker in a vector so that it can be joined to create
/// the full file. TODO: IS THIS A LITTLE EXPENSIVE
pub fn return_vector(
    trace: &mut TraceAgent<OrdKeySpine<model::Triple, usize, isize>>,
    time: usize
) -> Vec<String>
{  
    use differential_dataflow::trace::TraceReader;
    use differential_dataflow::trace::cursor::Cursor;

    let mut res = Vec::new();

    if let Some((mut cursor, storage)) = trace.cursor_through(&[time]){
        while let Some(key) = cursor.get_key(&storage) {
            while let Some(&()) = cursor.get_val(&storage) {
                let mut count = 0;
                use timely::order::PartialOrder;
                cursor.map_times(&storage, |t, diff| {
                    // println!("{}, DIFF:{:?} ", key, diff);
                    if t.less_equal(&(time-1)) {
                        count += diff;
                    }
                });
                if count > 0 {
                    // println!("{}", key);
                    // key.print_easy_reading();
                    res.push(key.to_string());
                } 
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage);
        }
    } else {
        println!("COULDN'T GET CURSOR");
    }

    res
}

/// Concatenates the different fragments into the final file
pub fn save_concatenate(path: &str, result: Vec<Result<Vec<String>, String>>) {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut del_file = OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open(path)
        .expect("Something wrong happened with the ouput file");

    for i in 0..result.len() {
        if let Some(Ok(vec)) = result.get(i) {
            for elem in vec {
                // println!("{}", elem);
                if let Err(e) = writeln!(del_file, "{}", elem) {
                    eprintln!("Couldn't write to file: {}", e);
                }
            }
        }    
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

}
