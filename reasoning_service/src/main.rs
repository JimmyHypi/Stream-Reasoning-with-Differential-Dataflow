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

        reasoning_service::save_to_file_through_trace("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\incremental_materialization_easy_refactored.nt", &mut result_trace, 1);
        
        if index == 0 {
            println!("Saving to file time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        /*

        // Test purpose only -- Addition

        let t_box_batch = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\t_box_addition_test.nt", index, peers);
        let a_box_batch = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\a_box_addition_test.nt", index, peers);

        reasoning_service::add_data(a_box_batch, &mut data_input, t_box_batch, 2);
        
        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("First update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }


        reasoning_service::save_to_file_through_trace("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\incremental_materialization_easy_refactored.nt", &mut result_trace, 2);

        if index == 0 {
            println!("Saving to file first update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        */

        // Test purpose only -- Deletion
        // THE REMOVAL IS NOT THAT EASY, CUZ I HAVE TO REMOVE EVEN ALL THE OTHER TRIPLES THAT USED A TRIPLE THAT IS BEING REMOVED AT THAT TIME, OH GOD..

        let t_box_batch = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\t_box_deletion_test.nt", index, peers);
        // let a_box_changes = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\a_box_addition_test.nt", index, peers);
        
        reasoning_service::remove_data(vec![], &mut data_input, t_box_batch, 2);
        
        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("Second update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }


        reasoning_service::save_to_file_through_trace("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Lehigh_University_Benchmark\\incremental_materialization_easy_refactored.nt", &mut result_trace, 2);

        if index == 0 {
            println!("Saving to file second update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }



    }).expect("Couldn't run timely dataflow correctly");

}