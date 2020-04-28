#![deny(missing_docs)]
//! This binary runs the reasoning service on a example knowledge base.  
fn main(){

    timely::execute_from_args(std::env::args(), move |worker| {
        use differential_dataflow::input::Input;  

        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let t_box_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example\\example_data\\univ-bench-oversimple-no-owl.owl";
        let a_box_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example\\example_data\\simple_abox.nt";
        let t_box_addition_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example\\example_data\\t_box_addition_test.nt";
        let a_box_addition_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example\\example_data\\a_box_addition_test.nt";
        let full_materialization_output_path = format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example\\result\\full_materialization\\full_materialization_peers{}\\example_worker{}.nt", peers, index);
        let incremental_materialization_addition_output_path = format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\example\\result\\with_addition\\with_addition_peers{}\\example_additions_worker{}.nt", peers, index);
        
        // Track progress
        let mut probe = timely::dataflow::ProbeHandle::new();

        let (mut data_input, mut result_trace) = worker.dataflow::<usize,_,_>(|scope| {

            let (data_input, data_collection) = scope.new_collection::<reasoning_service::model::Triple,_>();

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

        reasoning_service::save_to_file_through_trace(&full_materialization_output_path, &mut result_trace, 1);
        
        if index == 0 {
            println!("Saving to file time: {}ms", timer.elapsed().as_millis());
        }

        // Addition 

        // WARNING: Shouldn't this be the same as for removals? where everyworker should add the same data?
        let t_box_batch = reasoning_service::load_data(t_box_addition_path, index, peers);
        let a_box_batch = reasoning_service::load_data(a_box_addition_path, index, peers);
        reasoning_service::add_data(a_box_batch, &mut data_input, t_box_batch, 2);

        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            println!("First update time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        reasoning_service::save_to_file_through_trace(&incremental_materialization_addition_output_path, &mut result_trace, 2);

        if index == 0 {
            println!("Saving to file first update time: {}ms", timer.elapsed().as_millis());
        }

    }).expect("Couldn't run timely dataflow correctly");

}