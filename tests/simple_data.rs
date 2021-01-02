use differential_dataflow::input::Input;
use log::{debug, error, info};
use reasoning_service::encoder::{
    BiMapEncoder, BiMapTrait, EncodedTriple, Encoder, NTriplesParser, ParserTrait, SimpleLogic,
};
use reasoning_service::model::{
    RDFS_DOMAIN, RDFS_RANGE, RDFS_SUB_CLASS_OF, RDFS_SUB_PROPERTY_OF, RDF_TYPE,
};
use std::rc::Rc;

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

        let mut parser = NTriplesParser::new_default();
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
        let (map, mut vec) = reasoning_service::load_data_same_encoder_different_encoded_dataset::<
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
        info!("{:#?}", rdfs_keywords);
        info!("String Map:\n{:#?}", map);
        info!("TBox Encoded:\n{:?}", t_data);
        info!("ABox Encoded:\n{:#?}", a_data);
        let (mut data_input, mut result_trace) = worker.dataflow::<usize, _, _>(|scope| {
            // [IMPROVEMENT]:
            // To get the type of the data in the dataflow I have to hard code it and put
            // EncodedTriple<u64>. I want to be able to infer it from the Encoder.
            // Another associated type "Item". One way could be to make the EncodedDataSet to
            // implement Iterator and target its associated type Item. But a simple Vec
            // does not implement Iterator..
            let (data_input, data_collection) =
                scope.new_collection::<reasoning_service::encoder::EncodedTriple<u64>, _>();

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

        /*reasoning_service::save_to_file_through_trace::<BiMapEncoder, _, _>(
            "NOT_USED",
            &mut result_trace,
            1,
        );*/

        let result = reasoning_service::return_vector::<BiMapEncoder, _, _>(&mut result_trace, 1);

        let translated = map.translate::<BiMapEncoder>(result);

        info!("{:#?}", translated);
    })
    .expect("Could not run timely dataflow correctly");
}
