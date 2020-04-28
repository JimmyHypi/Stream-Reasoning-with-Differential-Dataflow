#![allow(non_snake_case)]
#![deny(missing_docs)]
//! This binary runs the reasoning service on a example knowledge base.  
fn main(){
    use std::time::Instant;
    // We start the timer before the join so we get the full computation time
    let full_timer = Instant::now();

    // Read number of universities from command line argument
    let universities = std::env::args().nth(1).expect("Must prolvide number of universities").parse::<usize>().expect("Couldn't parse Sequences as an integer");

    // this variable contains the handles of the threads and allows you to perform the join
    // on thereads so we can safely output the data to file
    let worker_guard = 
        timely::execute_from_args(std::env::args(), move |worker| {
            use differential_dataflow::input::Input;  

            let mut timer = worker.timer();
            let index = worker.index();
            let peers = worker.peers();

            let t_box_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\univ-bench-prefix-changed.owl";
            let a_box_path = format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\LUBM\\LUBM{}\\data\\Universities.nt", universities);
            let t_box_addition_path = "C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\LUBM\\updates\\add_update6\\update6.nt";            
            
            // Track progress
            let mut probe = timely::dataflow::ProbeHandle::new();

            let (mut data_input, mut result_trace) = worker.dataflow::<usize,_,_>(|scope| {

                let (data_input, data_collection) = scope.new_collection::<reasoning_service::model::Triple,_>();

                let res_trace = reasoning_service::full_materialization(&data_collection, &mut probe);

                (data_input, res_trace)
                    
            });

            // Insertion of data in the dataflow

            let (t_box, a_box) = reasoning_service::load_lubm_data(&a_box_path, t_box_path, index, peers);
                    
            if index == 0 {
                println!("Load time: {}ms", timer.elapsed().as_millis());
                println!("Triples loaded: {}", a_box.len()+t_box.len());
                timer = std::time::Instant::now();
            }
            
            // here time advances to 1, cause initial data has timestamp 0.
            reasoning_service::insert_starting_data(a_box, &mut data_input, t_box);
            
            while probe.less_than(data_input.time()) {
                worker.step();
            }

            // For now only worker 0 then we should consider the one that took the most
            if index == 0 {
                println!("Full materialization time: {}ms", timer.elapsed().as_millis());
                timer = std::time::Instant::now();
            }

            // Addition 
            let t_box_batch = reasoning_service::load_data(t_box_addition_path, index, peers);
            reasoning_service::add_data(vec![], &mut data_input, t_box_batch, 2);

            while probe.less_than(data_input.time()) {
                worker.step();
            }

            if index == 0 {
                println!("Addition update time: {}ms", timer.elapsed().as_millis());
                timer = std::time::Instant::now();
            }

            let result = reasoning_service::return_vector(&mut result_trace, 2);

            if index == 0 {
                println!("Saving to vector first update time: {}ms", timer.elapsed().as_millis());
            }

            result

        }).expect("Couldn't run timely dataflow correctly");

    let result = worker_guard.join();
    let peers = result.len();
    let inc_materialization_output_path = format!("C:\\Users\\xhimi\\Documents\\University\\THESIS\\data\\LUBM\\LUBM{}\\result\\with_addition\\update6\\lubm{}_add_u6_peers{}.nt", universities, universities, peers);
    
    let timer = Instant::now();
    reasoning_service::save_concatenate(&inc_materialization_output_path, result);
    // To obtain the actual time to save we should sum the previous with this.
    // TODO: HERE WE SAVE EVERYTIME THE WHOLE DATASET. WE SHOULD SAVE ONLY THE ADDITION
    // AND CONCATENATE THEM TO THE FILE AND NOT COMPUTING THE FULL MATERIALIZATION ALL THE TIMES.
    println!("Saving to file incremental materialization: {}ms", timer.elapsed().as_millis());
    println!("The whole computation took: {}ms", full_timer.elapsed().as_millis());

}