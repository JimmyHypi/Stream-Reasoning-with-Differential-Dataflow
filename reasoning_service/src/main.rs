#![deny(missing_docs)]
//! The purpose of this project is to perform a reasoning service
//! using differential dataflow. This is based on the work of
//! DynamiTE, that we are going to use as a comparison.
 
// TODO: FIX ALL DOCUMENTATION COMMENTS, SO FAR ONLY A SKETCH OF IT
// TODO: check TODO, IMPORTANT, ASSUMPTION, ISSUE labels in the document and consider them

use timely::dataflow::operators::ToStream;
use timely::dataflow::operators::Map;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::Join;

fn main(){

    let test_mode = std::env::args().nth(1).unwrap() == "test";

    if test_mode {

        timely::execute_from_args(std::env::args(), |worker| {
            // Simple data contains both schema types and generic types of triples
            let simple_data = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\test_for_simple_reasoning.nt", 0, 1);
            let ruleset = reasoning_service::load_rules("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\rdfs.rules");
            for rule in ruleset { println!("{}", rule); }

            worker.dataflow::<usize,_,_>(|scope| {

                let simple_data_collection = 
                    simple_data
                        .to_stream(scope)
                        .map(|triple| (triple, 0, 1))
                        .as_collection()
                        ;
                
                let simple_data_collection_as_strings = 
                    simple_data_collection
                        .map(|triple| (triple.subject, triple.predicate, triple.object))
                        ;

                let spo_triples = 
                    simple_data_collection
                        .filter(|triple| triple.predicate == "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>")
                        // from p1 SPO p to (p1, p) preparing for the join operation
                        .map(|triple| (triple.subject, triple.object))
                        // .inspect(|x| println!("Saw: {:?}", (x.0).0))
                    ;
                
                use differential_dataflow::operators::iterate::Iterate;
                use differential_dataflow::operators::consolidate::Consolidate;

                // T(x, p, y) <= T(p1, SPO, p),T(x, p1, y)
                let net_additions =
                    simple_data_collection_as_strings
                        .iterate(|inner| {
                            let spo_triples_in = spo_triples.enter(&inner.scope());
                            let simple_data_collection_as_strings = simple_data_collection_as_strings.enter(&inner.scope());

                            simple_data_collection_as_strings
                                .concat(&inner)
                                .map(|(x, p1, y)| (p1, (x, y)))
                                .join(&spo_triples_in)
                                .map(|(_p1, ((x, y), p))| (x, p, y))
                        })
                        .consolidate()
                        ;

                net_additions
                    .inspect(|x| println!("{:?}", x))
                    ;
                
                
                
                
                
                
                
                // let not_spo_triples =
                //     simple_data_collection
                //         .filter(|triple| triple.predicate != "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>")
                //         // from x p1 y to (p1, (x, y)) preparing for the join operation
                //         .map(|triple| (triple.predicate, (triple.subject, triple.object)))
                //         .join(&spo_triples)
                //         // (p1, ((x, y), p))
                //         .map(|(_p1, ((x, y), p))| (x, p, y))
                //         .inspect(|x| println!("\n\n\nSaw: {:?}", x))
                //         ;

        
                        
            })


        }).expect("Couldn't run timely dataflow correctly");
        



    } else {
        let c = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Stream-Reasoning-with-Differential-Dataflow\\data\\University0_0.nt", 0, 1);
        let ontology = reasoning_service::load_ontology("C:\\Users\\xhimi\\Documents\\University\\THESIS\\univ-bench-preprocessed.owl");   
        // ISSUE: the main problem here is the fact that owl restrictions are not well represented
        // with the parser we used from crates.io but ARE THE BLANK NODES GOING TO CHANGE THE
        // RESONING WE ARE DOING, THAT'S ONLY RDFS REASONING SO IT SHOULD NOT BE A PROBLEM
        // the only thing that might happen is that we will find that an instance is class of 
        // something like this _:A0 that means that that is a resource represented as a blank node
        // so there actually is a problem..
        // SOLUTION: USE A SOFTWARE LIKE PROTEGE' TO REMOVE ALL THE TRIPLES NOT IN RDFS (BLANKNODES)
        // ARE A OWL THING NOT A RDFS SO NO PROBLEM.
        c.iter().for_each(|triple| {
            if triple.subject == "<http://www.Department0.University0.edu/FullProfessor7>" {
                // triple.print_easy_reading();
                println!("{}", triple);
            }
        });
    }

}