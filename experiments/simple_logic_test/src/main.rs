use reasoning_service::encoder::{BiMapEncoder, EncoderUnit, NTriplesParser, SimpleLogic};

fn main() {
    env_logger::init();
    let parser = NTriplesParser::new();
    // It sucks that this has a state, encoder needs to lock the resource all the times.
    let encoding_logic = SimpleLogic::new(0);
    let encoder: EncoderUnit<_, _, BiMapEncoder, _, _> = EncoderUnit::new(parser, encoding_logic);

    reasoning_service::run_materialization(encoder, move |data_input, mut probe, rdfs_keywords| {
        simple_logic_test::full_materialization(&data_input, &mut probe, &rdfs_keywords)
    })
    .expect("Could not run computation");
}
