#![deny(missing_docs)]
//! The purpose of this project is to perform a reasoning service
//! using differential dataflow. This is based on the work of
//! DynamiTE, that we are going to use as a comparison.
 
// TODO: FIX ALL DOCUMENTATION COMMENTS, SO FAR ONLY A SKETCH OF IT
// TODO: check TODO, IMPORTANT, ASSUMPTION, ISSUE labels in the document and consider them

// mod model;

fn main(){

    timely::execute_from_args(std::env::args(), |worker| {
        use differential_dataflow::input::Input;
        use differential_dataflow::operators::iterate::Iterate;
        use differential_dataflow::operators::Join;
        use differential_dataflow::operators::reduce::Threshold;
        use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
        use timely::dataflow::operators::probe::Probe;
        use differential_dataflow::trace::TraceReader;
        use differential_dataflow::trace::cursor::Cursor;  

        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers(); 

        // Track progress
        let mut probe = timely::dataflow::ProbeHandle::new();

        let (mut a_box_input, mut t_box_input, mut result_trace) = worker.dataflow::<usize,_,_>(|scope| {

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

            let sco_transitive_closure =        
                t_box
                    .filter(|triple| triple.predicate == reasoning_service::model::RDFS_SUB_CLASS_OF)
                    .iterate(|inner| {
                        
                        inner 
                            .map(|triple| (triple.object, (triple.subject, triple.predicate)))
                            .join(&inner.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                            .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))| 
                                reasoning_service::model::Triple {
                                    subject: subj1,
                                    predicate: pred1,
                                    object: obj2,
                                }
                            )
                            .concat(&inner)
                            .distinct()


                    })
                    ;

            /*******************************************************************************************************/
                

            /*******************************************************************************************************/
            /*                             T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)                               */
            /*******************************************************************************************************/
            let spo_transitive_closure = 
                t_box
                    .filter(|triple| triple.predicate == reasoning_service::model::RDFS_SUB_PROPERTY_OF)
                    .iterate(|inner| {
                                        
                        inner 
                            .map(|triple| (triple.object, (triple.subject, triple.predicate)))
                            .join(&inner.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                            .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))| 
                                reasoning_service::model::Triple {
                                    subject: subj1,
                                    predicate: pred1,
                                    object: obj2,
                                }
                            )
                            .concat(&inner)
                            .distinct()
                        
                        
                    })
                    ;

            /*******************************************************************************************************/


            /*******************************************************************************************************/
            /*                             T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)                             */
            /*******************************************************************************************************/

            let sco_type_rule = 
                    a_box
                        .filter(|triple| triple.predicate == reasoning_service::model::RDF_TYPE)
                        .iterate(|inner| {
                            let sco_transitive_closure_in =
                                 sco_transitive_closure
                                    .enter(&inner.scope())
                            ;

                            inner
                                .map(|triple| (triple.object, (triple.subject, triple.predicate)))
                                .join(&sco_transitive_closure_in.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                                .map(|(_key, ((x, typ), (_sco, b)))| 
                                    reasoning_service::model::Triple {
                                        subject: x,
                                        predicate: typ,
                                        object: b
                                    }
                                )
                                .concat(&inner)
                                .distinct()
                        })
                        ;

            /*******************************************************************************************************/
                

            /*******************************************************************************************************/
            /*                             T(x, p, b) <= T(p1, SPO, p),T(x, p1, y)                                 */
            /*******************************************************************************************************/

            let spo_type_rule = 
                    a_box
                        .iterate(|inner| {
                            let spo_transitive_closure_in =
                                 spo_transitive_closure
                                    .enter(&inner.scope())
                            ;

                            inner
                                .map(|triple| (triple.predicate, (triple.subject, triple.object)))
                                .join(&spo_transitive_closure_in.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                                .map(|(_key, ((x, y), (_spo, p)))| 
                                    reasoning_service::model::Triple {
                                        subject: x,
                                        predicate: p,
                                        object: y,
                                    }
                                )
                                .concat(&inner)
                                .distinct()
                        })
            ;

            /*******************************************************************************************************/


            /*******************************************************************************************************/
            /*                           T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)                               */
            /*******************************************************************************************************/
                
            let only_domain =
                t_box
                    .filter(|triple| triple.predicate == reasoning_service::model::RDFS_DOMAIN)
                    ;

            let domain_type_rule = 
                    a_box
                        .iterate(|inner| {
                            let only_domain_in =
                                 only_domain
                                    .enter(&inner.scope())
                            ;

                            inner
                                .map(|triple| (triple.predicate, (triple.subject, triple.object)))
                                .join(&only_domain_in.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                                .map(|(_key, ((a, _b), (_dom, d)))| 
                                    reasoning_service::model::Triple {
                                        subject: a,
                                        predicate: String::from(reasoning_service::model::RDF_TYPE),
                                        object: d
                                    }
                                )
                                .concat(&inner)
                                .distinct()
                        })
                        ;

            /*******************************************************************************************************/

            /*******************************************************************************************************/
            /*                           T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)                               */
            /*******************************************************************************************************/
                            
            let only_range =
                t_box
                    .filter(|triple| triple.predicate == reasoning_service::model::RDFS_RANGE)
                    ;

            let range_type_rule = 
                    a_box
                        .iterate(|inner| {
                            let only_range_in =
                                 only_range
                                    .enter(&inner.scope())
                            ;

                            inner
                                // .map(|(a, p, b)| (p, (a, b)))
                                .map(|triple| (triple.predicate, (triple.subject, triple.object)))
                                // .join(&only_range_in.map(|(p, ran, r)| (p, (ran, r))))
                                .join(&only_range_in.map(|triple| (triple.subject, (triple.predicate, triple.object))))
                                // .map(|(_key, ((_a, b), (_ran, r)))| (b, String::from("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"), r))
                                .map(|(_key, ((_a, b), (_ran, r)))| 
                                    reasoning_service::model::Triple {
                                        subject: b,
                                        predicate: String::from(reasoning_service::model::RDF_TYPE),
                                        object: r
                                    }
                                )
                                .concat(&inner)
                                .distinct()
                        })         
                    ;
             
            /*******************************************************************************************************/

                
            let arrangement = 
                sco_transitive_closure
                    .concat(&spo_transitive_closure)
                    .concat(&sco_type_rule)
                    .concat(&spo_type_rule)
                    .concat(&domain_type_rule)
                    .concat(&range_type_rule)
                    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!VERY INEFFICIENT!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    // TODO: FIX THIS NONSENSE, IT'S NEEDED BECAUSE WHEN I INSERT A NEW TRIPLE IT DOES NOT END IN THE
                    // MATERIALIZATION FILE
                    // Actually.. I don't know if it's as bad as I thought..
                    .concat(&t_box)
                    .concat(&a_box)
                    // TODO: Impement all the traits necessary to apply distinct directly on collections of triples
                    .map(|triple| (triple.subject, triple.predicate, triple.object))
                    .distinct()
                    .map(|(x, y, j)| {
                        reasoning_service::model::Triple {
                            subject: x,
                            predicate: y,
                            object: j,
                        }
                    })
                    // .inspect(|triple| (triple.0).print_easy_reading())
                    .arrange_by_self()
                    ;


            arrangement
                .stream
                .probe_with(&mut probe)
                ;

            (a_box_input, t_box_input, arrangement.trace)
                
        });


        let a_box = reasoning_service::load_data(&format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\LUB1_nt_consolidated\\Universities.nt"), index, peers);

        // let a_box = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\test_for_simple_reasoning.nt", index, peers);

        if index == 0 {
            println!("ABox triples: {}", a_box.len());
        }
            
        let t_box = reasoning_service::load_ontology("C:\\Users\\xhimi\\Documents\\University\\THESIS\\univ-bench-prefix-changed.owl");
        // let t_box = reasoning_service::load_ontology("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\univ-bench-oversimple.owl");

        
        
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

        a_box_input.advance_to(1); a_box_input.flush();
        t_box_input.advance_to(1); t_box_input.flush();

        while probe.less_than(a_box_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("Full materialization time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        // Save the full materialization fo file
        use std::fs::OpenOptions;
        use std::io::Write;

        let mut full_materialization_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\full_materialization_LUB1_with_prefix_changed.nt")
            // .open("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\full_materialization_easy.nt")
            .expect("Something wrong happened with the ouput file");


        // TODO: 
        // I want to transfer to file, but concurrently kinda stinks.. Cause the file gets all messed up.. let's first
        // transfer it in memory to something in the main thread and then figure out how to make it concurrency
        // FOR NOW I AM RUNNING THE PROGRAM WITH ONLY ONE THREAD. CUZ I WANT TO SEE IF THE MATERIALIZATION WORKS
        if let Some((mut cursor, storage)) = result_trace.cursor_through(&[1]){
            while let Some(key) = cursor.get_key(&storage) {
                while let Some(&()) = cursor.get_val(&storage) {
                    // println!("{}", key);
                    // key.print_easy_reading();
                    if let Err(e) = writeln!(full_materialization_file, "{}", key.to_string()) {
                        eprintln!("Couldn't write to file: {}", e);
                    }
                    cursor.step_val(&storage);
                }
                cursor.step_key(&storage);
            }
        } else {
            println!("COULDN'T GET CURSOR");
        }

        if index == 0 {
            println!("Saving to file time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }



        // Test purpose only -- Addition
        // Here we are adding the fact that graduate student is a sub class of student to test the queries that failed

        // <http://swat.cse.lehigh.edu/onto/univ-bench.owl#UndergraduateStudent> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://swat.cse.lehigh.edu/onto/univ-bench.owl#Student> .


        let triple_to_insert = reasoning_service::model::Triple {
            subject: String::from("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>"),
            predicate: String::from("<http://www.w3.org/2000/01/rdf-schema#subClassOf>"),
            object: String::from("<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>"),
        };


        // let t_box_changes = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\t_box_addition_test.nt", index, peers);
        // let a_box_changes = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\a_box_addition_test.nt", index, peers);

        // t_box_input.insert(t_box_changes[0].to_owned());
        t_box_input.insert(triple_to_insert.to_owned());
        t_box_input.advance_to(2); t_box_input.flush();
        a_box_input.advance_to(2); a_box_input.flush();

        
        while probe.less_than(t_box_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("First update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        // Save the updated materialization fo file
        let mut inc_materialization_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\incremental_materialization_LUB1_with_prefix_changed.nt")
            .expect("Something wrong happened with the ouput file");


        // TODO: 
        // I want to transfer to file, but concurrently kinda stinks.. Cause the file gets all messed up.. let's first
        // transfer it in memory to something in the main thread and then figure out how to make it concurrency
        // FOR NOW I AM RUNNING THE PROGRAM WITH ONLY ONE THREAD. CUZ I WANT TO SEE IF THE MATERIALIZATION WORKS
        if let Some((mut cursor, storage)) = result_trace.cursor_through(&[2]){
            while let Some(key) = cursor.get_key(&storage) {
                while let Some(&()) = cursor.get_val(&storage) {
                    // println!("{}", key);
                    // key.print_easy_reading();
                    if let Err(e) = writeln!(inc_materialization_file, "{}", key.to_string()) {
                        eprintln!("Couldn't write to file: {}", e);
                    }
                    cursor.step_val(&storage);
                }
                cursor.step_key(&storage);
            }
        } else {
            println!("COULDN'T GET CURSOR");
        }

        if index == 0 {
            println!("Saving to file first update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }




        // // Test purpose only --Addition
        // let t_box_changes = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\t_box_addition_test.nt", index, peers);
        // let a_box_changes = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\a_box_addition_test.nt", index, peers);


        // for round in 1..t_box_changes.len()+1 {

        //     t_box_input.insert(t_box_changes[round-1].to_owned());
        //     t_box_input.advance_to(round+1); t_box_input.flush();
        //     a_box_input.advance_to(round+1); a_box_input.flush();

        //     while probe.less_than(t_box_input.time()) {
        //         worker.step();
        //     }

        //     if index == 0 {
        //         println!("Update time: {}ms", timer.elapsed().as_millis());
        //         timer = std::time::Instant::now();
        //     }

        // } 
        
        // for round in 0..a_box_changes.len() {

        //     a_box_input.insert(a_box_changes[round].to_owned());
        //     t_box_input.advance_to(round+5); t_box_input.flush();
        //     a_box_input.advance_to(round+5); a_box_input.flush();

        //     while probe.less_than(a_box_input.time()) {
        //         worker.step();
        //     }

        //     if index == 0 {
        //         println!("Update time: {}ms", timer.elapsed().as_millis());
        //         timer = std::time::Instant::now();
        //     }

        // Test purpose only -- deletions
        // let t_box_changes = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\t_box_deletion_test.nt", index, peers);
        // let a_box_changes = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\a_box_deletion_test.nt", index, peers);


        // for round in 1..t_box_changes.len()+1 {

        //     t_box_input.remove(t_box_changes[round-1].to_owned());
        //     t_box_input.advance_to(round+1); t_box_input.flush();
        //     a_box_input.advance_to(round+1); a_box_input.flush();

        //     while probe.less_than(t_box_input.time()) {
        //         worker.step();
        //     }

        //     if index == 0 {
        //         println!("Update time: {}ms", timer.elapsed().as_millis());
        //         timer = std::time::Instant::now();
        //     }

        // } 
        
        // for round in 0..a_box_changes.len() {

        //     a_box_input.remove(a_box_changes[round].to_owned());
        //     t_box_input.advance_to(round+5); t_box_input.flush();
        //     a_box_input.advance_to(round+5); a_box_input.flush();

        //     while probe.less_than(a_box_input.time()) {
        //         worker.step();
        //     }

        //     if index == 0 {
        //         println!("Update time: {}ms", timer.elapsed().as_millis());
        //         timer = std::time::Instant::now();
        //     }

        // }


    }).expect("Couldn't run timely dataflow correctly");
        
}