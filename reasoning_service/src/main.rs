#![deny(missing_docs)]
//! The purpose of this project is to perform a reasoning service
//! using differential dataflow. This is based on the work of
//! DynamiTE, that we are going to use as a comparison.
 
// TODO: FIX ALL DOCUMENTATION COMMENTS, SO FAR ONLY A SKETCH OF IT
// TODO: check TODO, IMPORTANT, ASSUMPTION, ISSUE labels in the document and consider them

mod model;

fn main(){

    let test_mode = std::env::args().nth(1).unwrap() == "test";

    if test_mode {

        timely::execute_from_args(std::env::args(), |worker| {
            use differential_dataflow::input::Input;
            use differential_dataflow::operators::iterate::Iterate;
            use differential_dataflow::operators::Join;
            use differential_dataflow::operators::reduce::Threshold;


            let mut timer = worker.timer();
            let index = worker.index();
            let peers = worker.peers(); 

            // Track progress
            // let mut probe = timely::dataflow::ProbeHandle::new();

            let (mut a_box_input, mut t_box_input) = worker.dataflow::<usize,_,_>(|scope| {

                let (a_box_input, a_box) = scope.new_collection::<reasoning_service::model::Triple,_>();
                let (t_box_input, t_box) = scope.new_collection::<reasoning_service::model::Triple,_>();

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
                



                /*******************************************************************************************************/
                /*                             T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)                               */
                /*******************************************************************************************************/

                let only_sco =
                    t_box
                        .filter(|triple| triple.predicate == "<http://www.w3.org/2000/01/rdf-schema#subClassOf>")
                        //.inspect(|triple| (triple.0).print_easy_reading())
                        ;

                let sco_transitive_closure =        
                    t_box
                        .filter(|triple| triple.predicate == "<http://www.w3.org/2000/01/rdf-schema#subClassOf>")
                        .map(|triple| (triple.subject, triple.predicate, triple.object))
                        .iterate(|inner| {
                        
                            let only_sco_in = only_sco.enter(&inner.scope());

                            inner 
                                .map(|(subj, pred, obj)| (obj, (subj, pred)))
                                .join(&inner.map(|(subj, pred, obj)| (subj, (pred, obj))))
                                .map(|(obj, ((subj1, pred1), (pred2, obj2)))| (subj1, pred1, obj2))
                                .concat(&inner)
                                .distinct()


                        })
                        // .inspect(|triple| println!("{:?}", triple))
                        .map(|(x, y, j)| {
                            reasoning_service::model::Triple {
                                subject: x,
                                predicate: y,
                                object: j,
                            }
                        })
                        // .inspect(|triple| (triple.0).print_easy_reading())
                        ;


                /*******************************************************************************************************/
                



                /*******************************************************************************************************/
                /*                             T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)                               */
                /*******************************************************************************************************/

                let only_spo =
                t_box
                    .filter(|triple| triple.predicate == "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>")
                    //.inspect(|triple| (triple.0).print_easy_reading())
                    ; 

                let spo_transitive_closure = 
                    t_box
                        .filter(|triple| triple.predicate == "<http://www.w3.org/2000/01/rdf-schema#subPropertyOf>")
                        .map(|triple| (triple.subject, triple.predicate, triple.object))
                        .iterate(|inner| {
                        
                            let only_spo_in = only_spo.enter(&inner.scope());
                        
                            inner 
                                .map(|(subj, pred, obj)| (obj, (subj, pred)))
                                .join(&inner.map(|(subj, pred, obj)| (subj, (pred, obj))))
                                .map(|(obj, ((subj1, pred1), (pred2, obj2)))| (subj1, pred1, obj2))
                                .concat(&inner)
                                .distinct()
                        
                        
                        })
                        // .inspect(|triple| println!("{:?}", triple))
                        .map(|(x, y, j)| {
                            reasoning_service::model::Triple {
                                subject: x,
                                predicate: y,
                                object: j,
                            }
                        })
                        // .inspect(|triple| (triple.0).print_easy_reading())
                        ;


                (a_box_input, t_box_input)

            });


            let a_box = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\test_for_simple_reasoning.nt", index, peers);
            let t_box = reasoning_service::load_ontology("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\univ-bench-oversimple.owl");

            if index == 0 {
                println!("Load time: {}ms", timer.elapsed().as_millis());
                timer = std::time::Instant::now();
            }

            for triple in a_box {
                a_box_input.insert(triple);
            }

            for triple in t_box {
                t_box_input.insert(triple);
            }

        }).expect("Couldn't run timely dataflow correctly");







































        /* timely::execute_from_args(std::env::args(), |worker| {
            // Simple data contains both schema types and generic types of triples, just for testing purpose
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
                    .map(|(x, p, z)|{
                        model::Triple {
                            subject: x,
                            predicate: p,
                            object: z,
                         }
                    })
                    .inspect(|x| println!("{:?}", x))
                    ;

            })


        }).expect("Couldn't run timely dataflow correctly");*/
        



    } else {
        let c = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Stream-Reasoning-with-Differential-Dataflow\\data\\University0_0.nt", 0, 1);
        let ontology = reasoning_service::load_ontology("C:\\Users\\xhimi\\Documents\\University\\THESIS\\univ-bench.owl");   
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