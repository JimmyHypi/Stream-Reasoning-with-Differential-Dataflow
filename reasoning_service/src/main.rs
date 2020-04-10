#![deny(missing_docs)]
//! The purpose of this project is to perform a reasoning service
//! using differential dataflow. This is based on the work of
//! DynamiTE, that we are going to use as a comparison.
 
// TODO: FIX ALL DOCUMENTATION COMMENTS, SO FAR ONLY A SKETCH OF IT
// TODO: check TODO, IMPORTANT, ASSUMPTION, ISSUE labels in the document and consider them

// mod model;

fn main(){
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::cell::RefCell;

    timely::execute_from_args(std::env::args(), move |worker| {
        use differential_dataflow::input::Input;  

        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers(); 




        // Track progress
        let mut probe = timely::dataflow::ProbeHandle::new();

        let (mut a_box_input, mut t_box_input, mut result_trace) = worker.dataflow::<usize,_,_>(|scope| {

            let (a_box_input, a_box) = scope.new_collection::<reasoning_service::model::Triple,_>();
            let (t_box_input, t_box) = scope.new_collection::<reasoning_service::model::Triple,_>();

            let res_trace = reasoning_service::full_materialization(&t_box, &a_box,&mut probe);

            (a_box_input, t_box_input, res_trace)
                
        });

        let (t_box, a_box) = reasoning_service::load_lubm_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\test_for_simple_reasoning.nt", "C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\univ-bench-oversimple.owl", index, peers);
        
        if index == 0 {
            println!("Load time: {}ms", timer.elapsed().as_millis());
            println!("Abox triples number: {}", a_box.len());
            timer = std::time::Instant::now();
        }
        
        // here time advances to 1, cause initial data as timestamp 0
        reasoning_service::insert_starting_data(a_box, &mut a_box_input, t_box, &mut t_box_input);
        
        while probe.less_than(a_box_input.time()) {
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
        // let a_box_changes = reasoning_service::load_data("C:\\Users\\xhimi\\Documents\\University\\THESIS\\Data_for_reasoning\\test_for_simple_reasoning\\a_box_addition_test.nt", index, peers);

        reasoning_service::add_data(vec![], &mut a_box_input, t_box_batch, &mut t_box_input, 2);
        
        while probe.less_than(t_box_input.time()) {
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
        
        reasoning_service::remove_data(vec![], &mut a_box_input, t_box_batch, &mut t_box_input, 1);
        
        while probe.less_than(t_box_input.time()) {
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