use reasoning_service::encoder::{BiMapEncoder, EncoderUnit, NTriplesParser, SimpleLogic};
use reasoning_service::model::{
    RDFS_DOMAIN, RDFS_RANGE, RDFS_SUB_CLASS_OF, RDFS_SUB_PROPERTY_OF, RDF_TYPE,
};
use std::sync::{Arc, Mutex};

fn main() {
    env_logger::init();
    let parser = NTriplesParser::new();
    let encoding_logic = SimpleLogic::new(0);
    let encoder: Arc<Mutex<EncoderUnit<_, _, BiMapEncoder, _, _>>> =
        Arc::new(Mutex::new(EncoderUnit::new(parser, encoding_logic)));

    reasoning_service::run_materialization(encoder.clone(), move |data_input, mut probe| {
        let mut locked = encoder.lock().unwrap();
        let rdfs_keywords = [
            *locked.get_right_from_map(Arc::new(String::from(RDFS_SUB_CLASS_OF))),
            *locked.get_right_from_map(Arc::new(String::from(RDFS_SUB_PROPERTY_OF))),
            *locked.get_right_from_map(Arc::new(String::from(RDF_TYPE))),
            *locked.get_right_from_map(Arc::new(String::from(RDFS_DOMAIN))),
            *locked.get_right_from_map(Arc::new(String::from(RDFS_RANGE))),
        ];
        simple_logic_test::full_materialization(&data_input, &mut probe, &rdfs_keywords)
    })
    .expect("Could not run computation");
}
