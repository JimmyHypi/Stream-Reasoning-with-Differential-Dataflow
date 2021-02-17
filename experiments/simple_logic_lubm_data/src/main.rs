use reasoning_service::encoder::{BiMapEncoder, EncoderUnit, NTriplesParser, SimpleLogic};
use reasoning_service::eval::output_figures;
use reasoning_service::Args;
use structopt::StructOpt;

fn main() {
    env_logger::init();
    let parser = NTriplesParser::new();
    // It sucks that this has a state, encoder needs to lock the resource all the times.
    let encoding_logic = SimpleLogic::new(0);
    let encoder: EncoderUnit<_, _, BiMapEncoder, _, _> = EncoderUnit::new(parser, encoding_logic);

    reasoning_service::run_materialization(encoder, move |data_input, mut probe, rdfs_keywords| {
        simple_logic_lubm_data::full_materialization(&data_input, &mut probe, &rdfs_keywords)
    })
    .expect("Could not run computation");

    // This requires StructOpt library, although it's just used in the reasoning_service library.
    // This dirties the interface a little bit.
    let args = Args::from_args();

    output_figures(args.output_folder);

    for path in args.incremental_file_paths {
        output_figures(path.0);
    }

    simple_logic_lubm_data::plot_uni_graph();
}
