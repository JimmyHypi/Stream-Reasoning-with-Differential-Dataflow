#![deny(missing_docs)]
//! This binary runs the reasoning service on a example knowledge base.  
fn main(){
    let guards = 
    timely::execute_from_args(std::env::args(), move |worker| {
        use differential_dataflow::input::Input;  

        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let universities = std::env::args().nth(1).expect("Must prolvide number of universities").parse::<usize>().expect("Couldn't parse Sequences as an integer");
        println!("\n\n{}\n\n\n", universities);
        let t_box_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example_easy\\example_data\\univ-bench-oversimple-no-owl.owl";
        let a_box_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example_easy\\example_data\\simple_abox.nt";
        let t_box_deletion_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example_easy\\example_data\\t_box_deletion_test.nt";
        let a_box_deletion_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example_easy\\example_data\\a_box_deletion_test.nt";
        
        
        // Track progress
        let mut probe = timely::dataflow::ProbeHandle::new();

        let (mut data_input, mut result_trace) = worker.dataflow::<usize,_,_>(|scope| {

            let (data_input, data_collection) = scope.new_collection::<reasoning_service::model::Triple,_>();

            // data_collection
            //     .inspect(|x| if (x.0).object == "<http://swat.cse.lehigh.edu/onto/univ-bench.owl#FullProfessor>" {println!("0: {:?}", x)})
            //     ;

            let res_trace = reasoning_service::full_materialization(&data_collection, &mut probe);

            (data_input, res_trace)
                
        });

        // Insertion of data in the dataflow

        let (t_box, a_box) = reasoning_service::load_lubm_data(a_box_path, t_box_path, index, peers);
                
        if index == 0 {
            println!("Load time: {}ms", timer.elapsed().as_millis());
            println!("Abox triples number: {}", a_box.len());
            timer = std::time::Instant::now();
        }
        
        // here time advances to 1, cause initial data has timestamp 0, confidently always.
        reasoning_service::insert_starting_data(a_box, &mut data_input, t_box);
        
        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("Full materialization time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        // let full_mat_vec = reasoning_service::return_vector(&mut result_trace, 1);
        reasoning_service::save_to_file_through_trace("", &mut result_trace, 1);
        
        if index == 0 {
            println!("Saving to file time: {}ms", timer.elapsed().as_millis());
        }

        
        // Deletion

        // Every worker has to remove the tuple from its local partition of the dataset. So we load the removing data.. or it could actually exchange it after using it.. mmh 
        // let t_box_batch_del = reasoning_service::load_data(t_box_deletion_path, 0, 1);
        let a_box_batch_del = reasoning_service::load_data(a_box_deletion_path, index, peers);
        
        reasoning_service::remove_data(a_box_batch_del, &mut data_input, vec![], 2);
        
        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("Second update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        let with_add_and_del_vec = reasoning_service::return_vector(&mut result_trace, 2);
        reasoning_service::save_to_file_through_trace("", &mut result_trace, 2);

        if index == 0 {
            println!("Saving to file second update time: {}ms", timer.elapsed().as_millis());
        }

        // TODO: FOR NOW THIS RETURNS A TUPLE OF THREE VECTORS BECAUSE WE ARE COMPUTING ALL THREE OPERATIONS
        // PLAN TO CHANGE THIS MAYBE IN A MORE INTERACTIVE WAY... (GUI? :D)
        with_add_and_del_vec

    }).expect("Couldn't run timely dataflow correctly");

    let result = guards.join();


    // Concatenating subresults for each worker


    // Probably this can be optimized through threads with a thread pooling and message passing
    // saving materialization to files, single threaded for now
    let peers = result.len();
    
    // let full_materialization_output_path = format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example_easy\\result\\full_materialization\\example_peers{}.nt", peers);
    // let incremental_materialization_addition_output_path = format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example_easy\\result\\with_addition\\example_additions_peers{}.nt", peers);
    // let incremental_materialization_addition_and_deletion_output_path = format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example_easy\\result\\with_addition_and_deletion\\example_deletion_peers{}.nt", peers);

    // reasoning_service::save_concatenate(&incremental_materialization_addition_and_deletion_output_path, result);
}