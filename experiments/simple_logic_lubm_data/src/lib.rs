use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::iterate::Iterate;
use differential_dataflow::operators::join::Join;
use differential_dataflow::operators::reduce::Threshold;
use differential_dataflow::Collection;
use timely::dataflow::Scope;

// [IMPROVEMENT]:
// Now, THIS IS WRONG, but for this simple case it works. Every rule needs to be parametrized
// on the type of the encoder and the encoded triple should be:
// <E::EncoderDataset as IntoIterator>::Item
type EncodedTriple<T> = (T, T, T);

/// First rule: T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)
// [IMPROVEMENT]:
// The current implementation of the function passes the translated value of S_C_O. My original
// idea was to pass parameter:
//      map: E::MapStructure,
// that as defined in the encoder module implements the BiMapTrait.
// This allows the filter operator to contain something like:
//      let v = if let Some(v) = map.get_right(&String::from(&model::S_C_O)) {
//          v
//      } else {
//          panic!("Throw error here");
//      };
//      triple.1 == v
// But this messes up all the lifetime as we would be required to pass the E::MapStructure inside
// the filter closure. This creates an odd error that I don't fully comprehend. Uncomment the next
// rule_1 function. To see the error and the overall situation.
// Passing the sco_value as a V would make the trait BiMapTrait useless..

pub fn rule_1<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    sco_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let sco_transitive_closure =
        data_collection
            //.filter(|triple| triple.predicate == model::RDFS_SUB_CLASS_OF)
            .filter(move |triple| triple.1 == sco_value )
            .iterate(|inner| {

                inner
                    .map(|triple| (triple.2, (triple.0, triple.1)))
                    .join(&inner.map(|triple| (triple.0, (triple.1, triple.2))))
                    .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))|
                        (subj1, pred1, obj2)
                    )
                    .concat(&inner)
                    .threshold(|_,c| { if c > &0 { 1 } else if c < &0 { -1 } else { 0 } })

            })
            //.inspect(|x| println!("AFTER_RULE_1: {:?}", x))
        ;

    sco_transitive_closure
}

/// Second rule: T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)
pub fn rule_2<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    spo_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let spo_transitive_closure = data_collection
        .filter(move |triple| triple.1 == spo_value)
        .iterate(|inner| {
            inner
                .map(|triple| (triple.2, (triple.0, triple.1)))
                .join(&inner.map(|triple| (triple.0, (triple.1, triple.2))))
                .map(|(_obj, ((subj1, pred1), (_pred2, obj2)))| (subj1, pred1, obj2))
                .concat(&inner)
                .threshold(|_, c| {
                    if c > &0 {
                        1
                    } else if c < &0 {
                        -1
                    } else {
                        0
                    }
                })
        });

    spo_transitive_closure
}

/// Third rule: T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)
pub fn rule_3<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    type_value: V,
    sco_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let sco_only = data_collection.filter(move |triple| triple.1 == sco_value);

    let candidates = data_collection
        .filter(move |triple| triple.1 == type_value)
        .map(|triple| (triple.2.clone(), (triple)))
        .join(&sco_only.map(|triple| (triple.0, ())))
        .map(|(_key, (triple, ()))| triple);

    let sco_type_rule = candidates.iterate(|inner| {
        let sco_only_in = sco_only.enter(&inner.scope());

        inner
            .map(|triple| (triple.2, (triple.0, triple.1)))
            .join(&sco_only_in.map(|triple| (triple.0, (triple.1, triple.2))))
            .map(|(_key, ((x, typ), (_sco, b)))| (x, typ, b))
            .concat(&inner)
            .threshold(|_, c| {
                if c > &0 {
                    1
                } else if c < &0 {
                    -1
                } else {
                    0
                }
            })
    });

    sco_type_rule
}

/// Fourth rule: T(x, p, b) <= T(p1, SPO, p),T(x, p1, y)
pub fn rule_4<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    spo_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    // Select only the triples whose predicate participates in a SPO triple
    let spo_only_out = data_collection.filter(move |triple| triple.1 == spo_value);

    let candidates = data_collection
        .map(|triple| ((triple.1.clone()), triple))
        .join(&spo_only_out.map(|triple| ((triple.0), ())))
        .map(|(_, (triple, ()))| triple);

    let spo_type_rule = candidates.iterate(|inner| {
        let spo_only = spo_only_out.enter(&inner.scope());
        inner
            .map(|triple| (triple.1, (triple.0, triple.2)))
            .join(&spo_only.map(|triple| (triple.0, (triple.1, triple.2))))
            .map(|(_key, ((x, y), (_spo, p)))| (x, p, y))
            .concat(&inner)
            .threshold(|_, c| {
                if c > &0 {
                    1
                } else if c < &0 {
                    -1
                } else {
                    0
                }
            })
    });
    spo_type_rule
}

/// Fifth rule: T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)
pub fn rule_5<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    domain_value: V,
    type_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let only_domain = data_collection.filter(move |triple| triple.1 == domain_value);

    let candidates = data_collection
        .map(|triple| ((triple.1.clone()), triple))
        .join(&only_domain.map(|triple| (triple.0, ())))
        .map(|(_, (triple, ()))| triple);

    // This does not require a iterative dataflow, the rule does not produce
    // terms that are used by the rule itself
    let domain_type_rule = candidates
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .join(&only_domain.map(|triple| (triple.0, (triple.1, triple.2))))
        .map(move |(_key, ((a, _b), (_dom, d)))| (a, type_value, d));

    domain_type_rule
}

/// Sixth rule: T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)
pub fn rule_6<G, V>(
    data_collection: &Collection<G, EncodedTriple<V>>,
    range_value: V,
    type_value: V,
) -> Collection<G, EncodedTriple<V>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    let only_range = data_collection.filter(move |triple| triple.1 == range_value);

    let candidates = data_collection
        .map(|triple| ((triple.1.clone()), triple))
        .join(&only_range.map(|triple| (triple.0, ())))
        .map(|(_, (triple, ()))| triple);

    // This does not require a iterative dataflow, the rule does not produce
    // terms that are used by the rule itself
    let domain_type_rule = candidates
        .map(|triple| (triple.1, (triple.0, triple.2)))
        .join(&only_range.map(|triple| (triple.0, (triple.1, triple.2))))
        .map(move |(_key, ((_a, b), (_ran, r)))| (b, type_value, r));

    domain_type_rule
}

use differential_dataflow::operators::arrange::arrangement::ArrangeBySelf;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use timely::dataflow::operators::probe::Probe;
use timely::dataflow::ProbeHandle;

/// Computes the full materialization of the collection
pub fn full_materialization<G, V>(
    data_input: &Collection<G, EncodedTriple<V>>,
    mut probe: &mut ProbeHandle<G::Timestamp>,
    // Contract:
    // rdfs_keywords[0] = sub_class_of
    // rdfs_keywords[1] = sub_property_of
    // rdfs_keywords[2] = sub_type
    // rdfs_keywords[3] = sub_domain
    // rdfs_keywords[4] = sub_range
    // [IMPROVEMENT]:
    // Maybe an HashMap here? Seems overkill still
    rdfs_keywords: &[V; 5],
) -> TraceAgent<OrdKeySpine<EncodedTriple<V>, G::Timestamp, isize>>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: std::cmp::Eq + std::hash::Hash + Clone + Copy + differential_dataflow::ExchangeData,
    EncodedTriple<V>: timely::Data + Ord + std::fmt::Debug,
{
    // ASSUMPTION: WE ARE HARDCODING THE RULES IN HERE
    // We only have two kinds of rules:
    // the ones that deal with only the T_box:
    // T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)
    // T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)
    // the ones that deal with both the a_box and the t_box
    // T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)
    // T(x, p, y) <= T(p1, SPO, p),T(x, p1, y)
    // T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)
    // T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)

    // Orders matters, to guarantee a correct execution of the materialization:
    // T(a, SCO, c) <= T(a, SCO, b),T(b, SCO, c)        -- rule_1
    // T(a, SPO, c) <= T(a, SPO, b),T(b, SPO, c)        -- rule_2
    // T(x, p, y) <= T(p1, SPO, p),T(x, p1, y)          -- rule_4
    // T(a, TYPE, D) <= T(p, DOMAIN, D),T(a, p, b)      -- rule_5
    // T(b, TYPE, R) <= T(p, RANGE, R),T(a, p, b)       -- rule_6
    // T(x, TYPE, b) <= T(a, SCO, b),T(x, TYPE, a)      -- rule_3
    // as we can see there is no rule with a literal in the body that
    // corresponds to a literal in the head of any subsequent rule

    let sco_transitive_closure = rule_1(&data_input, rdfs_keywords[0]);

    let spo_transitive_closure = rule_2(&data_input, rdfs_keywords[1]);

    let data_input = data_input
        .concat(&sco_transitive_closure)
        .concat(&spo_transitive_closure)
        //  VERY IMPORTANT: THE DISTINCT PUTS THE REMOVAL INTO ADDITION
        // SO WE REWRITE THE DISTINCT TO KEEP THE REMOVAL -1
        // .distinct()
        .threshold(|_, c| {
            if c > &0 {
                1
            } else if c < &0 {
                -1
            } else {
                0
            }
        });

    let spo_type_rule = rule_4(&data_input, rdfs_keywords[1]);

    let data_input = data_input.concat(&spo_type_rule).threshold(|_, c| {
        if c > &0 {
            1
        } else if c < &0 {
            -1
        } else {
            0
        }
    });

    let domain_type_rule = rule_5(&data_input, rdfs_keywords[3], rdfs_keywords[2]);

    // We don't need this, but still :P
    let data_input = data_input.concat(&domain_type_rule).threshold(|_, c| {
        if c > &0 {
            1
        } else if c < &0 {
            -1
        } else {
            0
        }
    });

    let range_type_rule = rule_6(&data_input, rdfs_keywords[4], rdfs_keywords[2]);

    let data_input = data_input.concat(&range_type_rule).threshold(|_, c| {
        if c > &0 {
            1
        } else if c < &0 {
            -1
        } else {
            0
        }
    });

    let sco_type_rule = rule_3(&data_input, rdfs_keywords[2], rdfs_keywords[0]);

    let data_input = data_input
        .concat(&sco_type_rule)
        // .distinct()
        .threshold(|_, c| {
            if c > &0 {
                1
            } else if c < &0 {
                -1
            } else {
                0
            }
        });

    let arrangement = data_input.arrange_by_self();

    arrangement.stream.probe_with(&mut probe);

    arrangement.trace
}

// Data processing relative to university

use reasoning_service::eval::{PlotInfo, Plotter};
use reasoning_service::Args;
use std::io::BufRead;
use std::io::BufReader;
use structopt::StructOpt;
use walkdir::WalkDir;
/// This function plots data relative to number of universities in the LUBM dataset.
/// For now:
/// 1) Best load time per Number of Universities
/// 2) Best materializaton time per number of universities
/// 3) Best save to file time per number of unversities
pub fn plot_uni_graph() {
    let mut data: [Vec<(f64, f64)>; 3] = [vec![], vec![], vec![]];
    let mut encoding_data: Vec<(f64, f64)> = vec![];
    let mut throughput_per_uni: [Vec<(f64, f64)>; 4] = [vec![], vec![], vec![], vec![]];

    let args = Args::from_args();

    let mut all_unis = args.output_folder;
    all_unis.pop();
    all_unis.pop();
    all_unis.pop();

    for entry in WalkDir::new(all_unis).min_depth(1).max_depth(1) {
        let entry = entry.expect("Failed to read file in stats path");
        if entry.path().is_dir() {
            let file_name = entry
                .path()
                .file_name()
                .expect("Could not get filename")
                .to_str();
            if is_uni_folder(file_name.expect("Could not convert to string.")) {
                get_uni_data(
                    entry.clone().path().to_path_buf(),
                    &mut data,
                    &mut encoding_data,
                    &mut throughput_per_uni,
                );
            }
        }
    }
    // The reasoning_service system saves the best data in the universities_x/output/stats under
    // load_best.txt, meaterialization_best.txt, save_to_file_best.txt. The first line tells you
    // the meaning of the data and the second line is the data: `number of workers, time`.
    let mut plotter = Plotter::new();

    let (encoding_time_min_val, encoding_time_max_val) = get_ends(&mut encoding_data);
    let (load_time_min_val, load_time_max_val) = get_ends(&mut data[0]);
    let (mat_time_min_val, mat_time_max_val) = get_ends(&mut data[1]);
    let (save_to_file_time_min_val, save_to_file_time_max_val) = get_ends(&mut data[2]);
    let (encoding_throughput_min_val, encoding_throughput_max_val) =
        get_ends(&mut throughput_per_uni[0]);
    let (load_throughput_min_val, load_throughput_max_val) = get_ends(&mut throughput_per_uni[1]);
    let (mat_throughput_min_val, mat_throughput_max_val) = get_ends(&mut throughput_per_uni[2]);
    let (sft_throughput_min_val, sft_throughput_max_val) = get_ends(&mut throughput_per_uni[3]);

    let encoding_y_range = reasoning_service::eval::compute_axis_range(
        encoding_time_min_val,
        encoding_time_max_val,
        0.4,
    );
    let load_y_range =
        reasoning_service::eval::compute_axis_range(load_time_min_val, load_time_max_val, 0.4);
    let mat_y_range =
        reasoning_service::eval::compute_axis_range(mat_time_min_val, mat_time_max_val, 0.4);
    let save_to_file_y_range = reasoning_service::eval::compute_axis_range(
        save_to_file_time_min_val,
        save_to_file_time_max_val,
        0.4,
    );

    let t_encoding_y_range = reasoning_service::eval::compute_axis_range(
        encoding_throughput_min_val,
        encoding_throughput_max_val,
        1.0,
    );
    let t_load_y_range = reasoning_service::eval::compute_axis_range(
        load_throughput_min_val,
        load_throughput_max_val,
        1.0,
    );
    let t_mat_y_range = reasoning_service::eval::compute_axis_range(
        mat_throughput_min_val,
        mat_throughput_max_val,
        1.0,
    );
    let t_sft_y_range = reasoning_service::eval::compute_axis_range(
        sft_throughput_min_val,
        sft_throughput_max_val,
        1.0,
    );

    data[0].sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare to Nan"));
    data[1].sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare to Nan"));
    data[2].sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare to Nan"));
    encoding_data.sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare to Nan"));
    throughput_per_uni[0]
        .sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare to Nan"));
    throughput_per_uni[1]
        .sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare to Nan"));
    throughput_per_uni[2]
        .sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare to Nan"));
    throughput_per_uni[3]
        .sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare to Nan"));

    let load_time_plot = plotter.generate_plot(data[0].to_owned());
    let mat_time_plot = plotter.generate_plot(data[1].to_owned());
    let save_to_file_time_plot = plotter.generate_plot(data[2].to_owned());
    let encoding_time_plot = plotter.generate_plot(encoding_data.to_owned());
    let encoding_throughput_plot = plotter.generate_plot(throughput_per_uni[0].to_owned());
    let load_throughput_plot = plotter.generate_plot(throughput_per_uni[1].to_owned());
    let mat_throughput_plot = plotter.generate_plot(throughput_per_uni[2].to_owned());
    let sft_throughput_plot = plotter.generate_plot(throughput_per_uni[3].to_owned());

    let load_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(load_y_range),
        x_label: "Universities",
        y_label: "Load Time (ms)",
    };
    let mat_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(mat_y_range),
        x_label: "Universities",
        y_label: "Materialization Time (s)",
    };
    let save_to_file_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(save_to_file_y_range),
        x_label: "Universities",
        y_label: "Save to File Time (s)",
    };
    let encoding_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(encoding_y_range),
        x_label: "Universities",
        y_label: "Encoding Time (s)",
    };
    let encoding_throughput_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(t_encoding_y_range),
        x_label: "Universities",
        y_label: "Throughput (KTriples/s)",
    };
    let load_throughput_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(t_load_y_range),
        x_label: "Universities",
        y_label: "Throughput (KTriples/s)",
    };
    let mat_throughput_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(t_mat_y_range),
        x_label: "Universities",
        y_label: "Throughput (KTriples/s)",
    };
    let sft_throughput_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(t_sft_y_range),
        x_label: "Universities",
        y_label: "Throughput (KTriples/s)",
    };
    plotter.save_plot(
        "load_time_per_uni.svg",
        vec![load_time_plot],
        load_plot_info,
    );
    plotter.save_plot("mat_time_per_uni.svg", vec![mat_time_plot], mat_plot_info);
    plotter.save_plot(
        "save_to_file_time_per_uni.svg",
        vec![save_to_file_time_plot],
        save_to_file_plot_info,
    );
    plotter.save_plot(
        "encoding_time_per_uni.svg",
        vec![encoding_time_plot],
        encoding_plot_info,
    );
    plotter.save_plot(
        "encoding_throughput_per_uni.svg",
        vec![encoding_throughput_plot],
        encoding_throughput_plot_info,
    );
    plotter.save_plot(
        "load_throughput_per_uni.svg",
        vec![load_throughput_plot],
        load_throughput_plot_info,
    );
    plotter.save_plot(
        "mat_throughput_per_uni.svg",
        vec![mat_throughput_plot],
        mat_throughput_plot_info,
    );
    plotter.save_plot(
        "save_to_file_throughput_per_uni.svg",
        vec![sft_throughput_plot],
        sft_throughput_plot_info,
    );
}

fn get_ends(vec: &mut Vec<(f64, f64)>) -> (f64, f64) {
    vec.sort_by(|(_, a), (_, b)| a.partial_cmp(b).expect("Tried to compare to NaN"));
    let len = vec.len();
    (vec[0].1, vec[len - 1].1)
}

fn is_uni_folder(folder: &str) -> bool {
    let index = if let Some(index) = folder.find('_') {
        index
    } else {
        return false;
    };
    "universities" == &folder[0..index]
}

fn get_uni_data(
    folder_path: std::path::PathBuf,
    vec_array: &mut [Vec<(f64, f64)>; 3],
    encoding_data: &mut Vec<(f64, f64)>,
    data_for_throughput: &mut [Vec<(f64, f64)>; 4],
) {
    let mut encoded_data_folder = folder_path.clone();
    let mut stats_folder = folder_path.clone();
    let mut universities_file = folder_path.clone();

    universities_file.push("Universities.nt");
    stats_folder.push("output/stats");
    encoded_data_folder.push("encoded_data");

    let encoding_stats = get_encoding_data(&encoded_data_folder, "average_encoding_time");
    let load_time = get_data(&stats_folder, "load_best.txt");
    let mat_time = get_data(&stats_folder, "materialization_best.txt");
    let save_to_file_time = get_data(&stats_folder, "save_to_file_best.txt");

    let uni_folder_file_name = folder_path
        .file_name()
        .expect("Could not read the name of the folder")
        .to_str()
        .expect("Could not convert uni_folder_name to &str");

    let number_of_unis = get_number_of_unis(uni_folder_file_name);

    if let Ok(avg) = encoding_stats {
        let number_of_triples = get_number_of_triples(&universities_file);
        if let Ok(n) = number_of_triples {
            data_for_throughput[0].push((number_of_unis as f64, n as f64 / avg));
        }
        encoding_data.push((number_of_unis as f64, avg / 1000.0));
    }
    if let Ok(load_time) = load_time {
        let number_of_triples = get_number_of_triples(&universities_file);
        if let Ok(n) = number_of_triples {
            data_for_throughput[1].push((number_of_unis as f64, n as f64 / load_time));
        }
        vec_array[0].push((number_of_unis as f64, load_time));
    }
    if let Ok(mat_time) = mat_time {
        let number_of_triples = get_number_of_triples(&universities_file);
        if let Ok(n) = number_of_triples {
            data_for_throughput[2].push((number_of_unis as f64, n as f64 / mat_time));
        }
        vec_array[1].push((number_of_unis as f64, mat_time / 1000.0));
    }
    if let Ok(save_to_file_time) = save_to_file_time {
        let number_of_triples = get_number_of_triples(&universities_file);
        if let Ok(n) = number_of_triples {
            data_for_throughput[3].push((number_of_unis as f64, n as f64 / save_to_file_time));
        }
        vec_array[2].push((number_of_unis as f64, save_to_file_time / 1000.0));
    }
}

fn get_number_of_triples(path: &std::path::PathBuf) -> Result<usize, String> {
    if let Ok(file) = std::fs::File::open(path) {
        let buf_read = BufReader::new(file);
        Ok(buf_read.lines().count())
    } else {
        return Err("Could not count the number of triples in the file.".to_string());
    }
}
fn get_encoding_data(path: &std::path::PathBuf, file_name: &str) -> Result<f64, String> {
    let mut p = path.clone();
    p.push(file_name);

    if let Ok(file) = std::fs::File::open(p) {
        let buf_read = BufReader::new(file);
        let mut lines = buf_read.lines().skip(1);
        let data = lines
            .next()
            .expect("Next returned nothing")
            .expect("Wrong format");
        Ok(data
            .parse::<f64>()
            .expect("Could not parse average encoding time"))
    } else {
        return Err("Could not read file".to_string());
    }
}
fn get_data(path: &std::path::PathBuf, file_name: &str) -> Result<f64, String> {
    let mut p = path.clone();
    p.push(file_name);

    if let Ok(file) = std::fs::File::open(p) {
        let buf_read = BufReader::new(file);
        let mut lines = buf_read.lines().skip(1);
        let data = lines
            .next()
            .expect("Next returned nothing")
            .expect("Wrong format");
        Ok(parse_data(data))
    } else {
        return Err("Could not read file".to_string());
    }
}

fn parse_data(string: String) -> f64 {
    let index = string.find(',').expect("Could not find `,`");
    let data = &string[index + 1..].trim();
    data.parse::<f64>().expect("Could not parse data")
}

fn get_number_of_unis(string: &str) -> usize {
    let index: usize = string
        .trim()
        .find('_')
        .expect("Could not find `_` separator");
    (string.trim())[index + 1..]
        .parse::<usize>()
        .expect("Could not parse number of universities")
}
