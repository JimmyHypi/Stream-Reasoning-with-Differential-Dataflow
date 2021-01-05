use differential_dataflow::input::Input;
use log::info;
use reasoning_service::encoder::{BiMapEncoder, BiMapTrait, Encoder, NTriplesParser, SimpleLogic};
use reasoning_service::model::{
    RDFS_DOMAIN, RDFS_RANGE, RDFS_SUB_CLASS_OF, RDFS_SUB_PROPERTY_OF, RDF_TYPE,
};
use std::rc::Rc;

type EncodedTriple<T> = (T, T, T);

#[test]
fn dataflow_integration_test() {
    env_logger::init();
    info!("Logger initialized");

    // [IMPROVEMENT]:
    // Use maybe lazy_static! and add a default.
    let t_box_filename = "data/univ-bench-oversimple-no-owl.nt";
    let a_box_filename = "data/test_for_simple_reasoning.nt";

    timely::execute_from_args(std::env::args(), move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut parser = NTriplesParser::new();
        let mut encoding_logic = SimpleLogic::new(0);

        // Track progress
        let mut probe = timely::dataflow::ProbeHandle::new();

        // Load the data so that we have both bijective maps and dataset.
        // Contract:
        // vec[0] = a_box encoded dataset
        // vec[1] = t_box encoded dataset
        // [IMPROVEMENT]:
        // The current implementation of the bimap does not use the index and peers variables.
        // Fix it to make it parallel
        let (mut map, mut vec) =
            reasoning_service::load_data_same_encoder_different_encoded_dataset::<
                BiMapEncoder,
                Rc<String>,
                u64,
                NTriplesParser,
                SimpleLogic,
            >(
                a_box_filename,
                t_box_filename,
                Some(index),
                Some(peers),
                &mut parser,
                &mut encoding_logic,
            );
        // According to the contract these are safe pops
        let t_data = vec.pop().expect("NO TBOX IN THE ENCODED DATASETS");
        let a_data = vec.pop().expect("NO ABOX IN THE ENCODED DATASETS");

        // [IMPROVEMENT]:
        // Here we are considering only the worker with index 0.. maybe an average among
        // all the workers?
        if index == 0 {
            info!("Load time: {}ms", timer.elapsed().as_millis());
            info!("A-Box Triples number: {}", a_data.len());
            timer = std::time::Instant::now();
        }
        let rdfs_keywords = [
            *map.get_right(&Rc::new(String::from(RDFS_SUB_CLASS_OF)))
                .expect("THERE IS NO SCO"),
            *map.get_right(&Rc::new(String::from(RDFS_SUB_PROPERTY_OF)))
                .expect("THERE IS NO SPO"),
            *map.get_right(&Rc::new(String::from(RDF_TYPE)))
                .expect("THERE IS NO TYPE"),
            *map.get_right(&Rc::from(String::from(RDFS_DOMAIN)))
                .expect("THERE IS NO DOMAIN"),
            *map.get_right(&Rc::from(String::from(RDFS_RANGE)))
                .expect("THERE IS NO RANGE"),
        ];

        let (mut data_input, mut result_trace) = worker.dataflow::<usize, _, _>(|scope| {
            // [IMPROVEMENT]:
            // To get the type of the data in the dataflow I have to hard code it and put
            // EncodedTriple<u64>. I want to be able to infer it from the Encoder.
            // Another associated type "Item". One way could be to make the EncodedDataSet to
            // implement Iterator and target its associated type Item. But a simple Vec
            // does not implement Iterator..
            let (data_input, data_collection) = scope.new_collection::<EncodedTriple<u64>, _>();

            let res_trace = reasoning_service::full_materialization::<_, Rc<String>, _>(
                &data_collection,
                &mut probe,
                &rdfs_keywords,
            );

            (data_input, res_trace)
        });

        // Time advances to 1.
        reasoning_service::insert_starting_data::<BiMapEncoder, _, _>(
            a_data,
            &mut data_input,
            t_data,
        );

        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            info!(
                "Full Materialization time: {}ms",
                timer.elapsed().as_millis()
            );
            timer = std::time::Instant::now();
        }

        reasoning_service::save_to_file_through_trace::<BiMapEncoder, _, _>(
            &map,
            "output/full_materialization_simple_data.nt",
            &mut result_trace,
            1,
        );

        if index == 0 {
            info!(
                "Saving to file time [Full Materialization]: {}ms",
                timer.elapsed().as_millis()
            );
        }

        // Addition Test

        let t_box_added = BiMapEncoder::insert_from_file(
            "data/t_box_addition_test.nt",
            &mut encoding_logic,
            &mut map,
            &mut parser,
            Some(index),
            Some(peers),
        )
        .unwrap();

        let a_box_added = BiMapEncoder::insert_from_file(
            "data/a_box_addition_test.nt",
            &mut encoding_logic,
            &mut map,
            &mut parser,
            Some(index),
            Some(peers),
        )
        .unwrap();

        reasoning_service::add_data::<BiMapEncoder, _, _>(
            a_box_added,
            &mut data_input,
            t_box_added,
            2,
        );

        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            info!("Addition Update Time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        reasoning_service::save_to_file_through_trace::<BiMapEncoder, _, _>(
            &map,
            "output/addition_materializatiion_simple_data.nt",
            &mut result_trace,
            2,
        );

        if index == 0 {
            info!(
                "Saving to file time [Addition]: {}ms",
                timer.elapsed().as_millis()
            );
            timer = std::time::Instant::now();
        }

        // Deletion Test:
        // Is it true that only TBox deletion can trigger more tuples to be deleted?
        // Although the data is removed the map retains the deleted triples and encoding.
        let t_box_removed = BiMapEncoder::insert_from_file(
            "data/t_box_deletion_test.nt",
            &mut encoding_logic,
            &mut map,
            &mut parser,
            Some(index),
            Some(peers),
        )
        .unwrap();

        reasoning_service::remove_data::<BiMapEncoder, _, _>(
            vec![],
            &mut data_input,
            t_box_removed,
            3,
        );

        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            info!("Deletion Update Time: {}ms", timer.elapsed().as_millis());
        }

        reasoning_service::save_to_file_through_trace::<BiMapEncoder, _, _>(
            &map,
            "output/deletion_materializatiion_simple_data.nt",
            &mut result_trace,
            3,
        );

        if index == 0 {
            info!(
                "Saving to file time [Deletion]: {}ms",
                timer.elapsed().as_millis()
            );
        }
    })
    .expect("Could not run timely dataflow correctly");
}
