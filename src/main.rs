#![deny(missing_docs)]
//! The purpose of this project is to perform a reasoning service
//! using differential dataflow. This is based on the work of
//! DynamiTE, that we are going to use as a comparison.
 
// TODO: FIX ALL DOCUMENTATION COMMENTS, SO FAR ONLY A SKETCH OF IT
// TODO: check TODO, IMPORTANT, ASSUMPTION, ISSUE labels in the document and consider them

// mod model;

fn main(){

    timely::execute_from_args(std::env::args(), move |worker| {
        use differential_dataflow::input::Input;  

        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers(); 

        // Track progress
        let mut probe = timely::dataflow::ProbeHandle::new();

        let (mut data_input, mut result_trace) = worker.dataflow::<usize,_,_>(|scope| {

            let (data_input, data_collection) = scope.new_collection::<reasoning_service::model::Triple,_>();

            let res_trace = reasoning_service::full_materialization(&data_collection, &mut probe);

            (data_input, res_trace)
                
        });


        // Insertion of data in the dataflow




        // let (t_box, a_box) = reasoning_service::load_lubm_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\LUB1_nt_consolidated\\Universities.nt", "C:\\Users\\xhimi\\Documents\\University\\THESIS\\univ-bench-prefix-changed.owl", index, peers);
        // simpler dataset to see what is going on
        // TODO: EVERY WORKER LOADS ALL THE ONTOLOGY. FIX
        let (t_box, a_box) = reasoning_service::load_lubm_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\test_for_simple_reasoning.nt", "C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\univ-bench-oversimple-no-owl.owl", index, peers);
                
        if index == 0 {
            println!("Load time: {}ms", timer.elapsed().as_millis());
            println!("Abox triples number: {}", a_box.len());
            timer = std::time::Instant::now();
        }
        
        // here time advances to 1, cause initial data as timestamp 0
        reasoning_service::insert_starting_data(a_box, &mut data_input, t_box);
        
        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("Full materialization time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        // reasoning_service::save_to_file_through_trace(&format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\Parallel_Materialization\\full_materialization\\LUB1_full_materialization_worker{}.nt", index), &mut result_trace, 1);
        reasoning_service::save_to_file_through_trace(&format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\Parallel_Materialization\\full_materialization\\running_example_worker{}.nt", index), &mut result_trace, 1);
        
        if index == 0 {
            println!("Saving to file time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        
        // Test purpose only -- Addition


        // WARNING: Shouldn't this be the same as for removals? where everyworker should add the same data?
        let t_box_batch = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\t_box_addition_test_another.nt", index, peers);
        // let a_box_batch = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\a_box_addition_test.nt", index, peers);
        reasoning_service::add_data(vec![], &mut data_input, t_box_batch, 2);

        
        // Simpler version to see what is going on
        // let t_box_batch = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\t_box_addition_test.nt", index, peers);
        // let a_box_batch = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\a_box_addition_test.nt", index, peers);

        // reasoning_service::add_data(a_box_batch, &mut data_input, t_box_batch, 2);


        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("First update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }


        // reasoning_service::save_to_file_through_trace(&format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\Parallel_Materialization\\materialization_after_additions\\LUB1_incremental_materialization_addition{}.nt", index), &mut result_trace, 2);
        reasoning_service::save_to_file_through_trace(&format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\Parallel_Materialization\\materialization_after_additions\\running_example_additions_worker{}.nt", index), &mut result_trace, 2);

        if index == 0 {
            println!("Saving to file first update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        

        // NOTHING TO BE REMOVED FOR NOW

        // Test purpose only -- Deletion

        // Every worker has to remove the tuple from its local partition of the dataset. So we load the removing data 
        let t_box_batch = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\t_box_deletion_test_another.nt", 0, 1);
        
        reasoning_service::remove_data(vec![], &mut data_input, t_box_batch, 2);
        
        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("Second update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        reasoning_service::save_to_file_through_trace(&format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\Parallel_Materialization\\materialization_after_deletions\\deletion_running_example_worker{}.nt", index), &mut result_trace, 2);

        if index == 0 {
            println!("Saving to file second update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

    }).expect("Couldn't run timely dataflow correctly");

}