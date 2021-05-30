//! Functions and definitions
#[macro_use]
extern crate lalrpop_util;

use crate::encoder::BiMapTrait;
use crate::eval::Statistics;
use crate::model::{RDFS_DOMAIN, RDFS_RANGE, RDFS_SUB_CLASS_OF, RDFS_SUB_PROPERTY_OF, RDF_TYPE};
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::{cursor::Cursor, TraceReader};
use differential_dataflow::{Collection, ExchangeData};
use log::info;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;
use timely::communication::allocator::generic::Generic;
use timely::dataflow::scopes::child::Child;
use timely::dataflow::ProbeHandle;
use timely::order::PartialOrder;
use timely::worker::Worker;

/// Encoder module
pub mod encoder;
use encoder::EncoderTrait;
use encoder::EncoderUnit;
use encoder::EncodingLogic;
use encoder::ParserTrait;
use encoder::Triple;

pub mod eval;
pub mod model;

fn parse_key_val<T, U, V>(s: &str) -> Result<(T, U, V), Box<dyn std::error::Error>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + 'static,
    U: std::str::FromStr,
    U::Err: std::error::Error + 'static,
    V: std::str::FromStr,
    V::Err: std::error::Error + 'static,
{
    let pos = s
        .find('=')
        .ok_or_else(|| format!("Invalid Path=Mode-Type: no `=` found in `{}`", s))?;
    let second_part = &s[pos + 1..];
    let p = second_part
        .find('_')
        .ok_or_else(|| format!("Invalid Path=Mode-Type: no `_` found in {}", s))?;
    Ok((
        s[..pos].parse()?,
        s[pos + 1..pos + p + 1].parse()?,
        s[pos + p + 2..].parse()?,
    ))
}

#[derive(StructOpt, Debug)]
pub struct Args {
    // Differential dataflow parameters
    #[structopt(short, long)]
    pub workers: Option<usize>,
    #[structopt(short = "n", long = "processes")]
    pub number_of_processes: Option<usize>,
    #[structopt(short = "p", long = "process")]
    pub process_id: Option<usize>,
    #[structopt(short, long)]
    pub hostfile: Option<std::path::PathBuf>,
    #[structopt(parse(from_os_str))]
    pub t_box_path: std::path::PathBuf,
    #[structopt(parse(from_os_str))]
    pub a_box_path: std::path::PathBuf,
    #[structopt(parse(from_os_str))]
    pub output_folder: std::path::PathBuf,
    #[structopt(
        name = "UPDATE",
        short = "u",
        long = "update",
        parse(try_from_str = parse_key_val),
        number_of_values = 1,
    )]
    pub incremental_file_paths: Vec<(std::path::PathBuf, IncrementalMode, IncrementalType)>,
}

#[derive(Debug, Clone)]
pub enum IncrementalMode {
    Addition,
    Deletion,
}
#[derive(Debug, Clone)]
pub enum IncrementalType {
    ABox,
    TBox,
}

// [IMPROVEMENT]:
// Error Handling.
// This should go in its own module
#[derive(Debug)]
pub struct ParseModeError {
    string: String,
}

impl From<String> for ParseModeError {
    fn from(s: String) -> Self {
        ParseModeError { string: s }
    }
}

impl std::fmt::Display for ParseModeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{}", self.string)
    }
}

impl std::error::Error for ParseModeError {}

impl std::str::FromStr for IncrementalMode {
    // [IMPROVEMENT]:
    // Error Handling here!
    type Err = ParseModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lc = s.to_lowercase();
        if lc == "i" || lc == "insert" || lc == "insertion" {
            Ok(IncrementalMode::Addition)
        } else if lc == "d" || lc == "delete" || lc == "deletion" {
            Ok(IncrementalMode::Deletion)
        } else {
            Err(format!("{} is not a correct mode [insert / deletion].", s).into())
        }
    }
}

impl std::str::FromStr for IncrementalType {
    // [IMPROVEMENT]:
    // Error Handling here!
    type Err = ParseModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lc = s.to_lowercase();
        if lc == "t" || lc == "t_box" || lc == "tbox" {
            Ok(IncrementalType::TBox)
        } else if lc == "a" || lc == "a_box" || lc == "abox" {
            Ok(IncrementalType::ABox)
        } else {
            Err(format!("{} is not a correct mode [insert / deletion].", s).into())
        }
    }
}

fn get_folder_name(path: std::path::PathBuf) -> String {
    let mut path_for_folder = path.clone();
    path_for_folder.pop();
    let filename = path
        .file_name()
        .expect("Could not get file name")
        .to_str()
        .expect("Could not get string");
    let index = filename.find('.').expect("Could not find `.` in path");
    let result = format!("encoded_data/{}_encoding/", &filename[0..index],);
    result
}
fn write_encoding_time(path: std::path::PathBuf, time: u128) {
    let mut path = path.clone();
    let file_path = get_folder_name(path.clone());
    path.pop();
    path.push(file_path);
    path.push("encoding_stats.txt");

    let mut file = crate::eval::open_append(path.clone());

    let metadata = std::fs::metadata(path.clone()).expect("Could not get metadata");
    // If the file is empty it means that it's the first iteration, we want
    // to append new results to previous results so to have more data to analyze
    if metadata.len() == 0 {
        writeln!(file, "Encoding Time (ms)").expect("Invalid File Path");
    }
    writeln!(file, "{}", time).expect("Could not write encoding time");

    let avg_encoding_time = get_avg_encoding_time(path.clone());

    path.pop();
    path.push("average_encoding_time.txt");
    let mut file = crate::eval::open_truncate(path);
    writeln!(file, "Average Encoding Time (ms)\n{}", avg_encoding_time)
        .expect("Could not write average encoding time");
}

use std::io::BufRead;
fn get_avg_encoding_time(path: std::path::PathBuf) -> f64 {
    let file = std::fs::File::open(path).expect("Could not open file");
    let reader = std::io::BufReader::new(file);
    let lines = reader.lines().skip(1);

    let mut count: usize = 0;
    let mut sum: usize = 0;

    for line in lines {
        count += 1;
        let line = line.expect("Could not read line!");
        let parsed = line.parse::<usize>().expect("Could not parse to usize");
        sum += parsed;
    }

    sum as f64 / count as f64
}

pub fn run_materialization<L, R, E, P, F, M>(
    mut encoder: EncoderUnit<L, R, E, P, F>,
    materialization: M,
) -> Result<(), String>
where
    // [IMPROVEMENT]:
    // The timely dataflow constraint require all of the components that get passed from
    // worker to worker to be 'static because we don't want the closure to outlive
    // the variable.. Is there another way around?
    R: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static + Clone + Copy,
    L: std::cmp::Eq
        + std::hash::Hash
        + std::fmt::Debug
        + std::fmt::Display
        + Send
        + Sync
        + 'static
        + From<String>,
    E: EncoderTrait<L, R> + 'static,
    <E::EncodedDataSet as IntoIterator>::Item: ExchangeData
        + Triple<R>
        + Ord
        + std::fmt::Debug
        + Clone
        + Copy
        + differential_dataflow::hashable::Hashable,
    P: ParserTrait<L> + 'static,
    F: EncodingLogic<L, R> + 'static,
    // [IMPROVEMENT]:
    // The dataflow timestamp is a usize, should it be anything else?
    M: Fn(
            &Collection<
                Child<'_, Worker<Generic>, usize>,
                <E::EncodedDataSet as IntoIterator>::Item,
            >,
            &mut ProbeHandle<usize>,
            // RDFS keywords. These might be needed
            &[R; 5],
        )
            -> TraceAgent<OrdKeySpine<<E::EncodedDataSet as IntoIterator>::Item, usize, isize>>
        + Send
        + Sync
        + 'static,
{
    let args = Arc::new(Args::from_args());
    let another_args = Args::from_args();
    let timely_args = get_timely_args(args.clone());
    let timely_params = TimelyParams {
        params: timely_args,
    };

    // [IMPORTANT]:
    // As of right now, the encoding is performed outside the dataflow computation, i.e. is not
    // parallel.
    // The encoder contains state that needs to be shared by the different workers. A possible
    // solution might be the use of message passing where all the workers communicate to a
    // thread their state change. Or locking the encoder. This last solution does not seem to be
    // efficient as ,in order to encode, a worker needs exclusive control over the encoder.
    // Moreover the encoding is persistent: it is first saved on a file and then read by the
    // worker, this for two reasons:
    // 1) Extend the functionalities to reuse the encoding withouth re-encoding (requires a
    //    read_encoding() function that is not implemented)
    // 2) Reading from within the timely dataflow worker closure is hard. Requires cloning a lot of
    //    very long vectors. One possible way of doing it is with channels where each thread
    //    receives one triple. Now this is a mess, I don't think diving into it is now worth it.
    //    My focus right now is on the materialization.
    let mut start = Instant::now();

    let t_box_encoded_path = encoder.encode_persistent(args.t_box_path.clone(), None, None);
    let encoding_time_tbox = start.elapsed().as_millis();
    info!("Persistent Encoding of TBox: {}ms", encoding_time_tbox);
    write_encoding_time(args.t_box_path.clone(), encoding_time_tbox);

    start = Instant::now();
    let a_box_encoded_path = encoder.encode_persistent(args.a_box_path.clone(), None, None);
    let encoding_time_abox = start.elapsed().as_millis();
    info!("Persistent Encoding of ABox: {}ms", encoding_time_abox);
    write_encoding_time(args.a_box_path.clone(), encoding_time_abox);

    start = Instant::now();
    // Get the encoding of the constant
    let rdfs_keywords = [
        *encoder.get_right_from_map(L::from(String::from(RDFS_SUB_CLASS_OF))),
        *encoder.get_right_from_map(L::from(String::from(RDFS_SUB_PROPERTY_OF))),
        *encoder.get_right_from_map(L::from(String::from(RDF_TYPE))),
        *encoder.get_right_from_map(L::from(String::from(RDFS_DOMAIN))),
        *encoder.get_right_from_map(L::from(String::from(RDFS_RANGE))),
    ];

    let mut update_paths = vec![];

    for (i, (path, a, b)) in args.incremental_file_paths.iter().enumerate() {
        let update_path = encoder.encode_persistent(path.clone(), None, None);
        let update_encoding_time = start.elapsed().as_millis();

        info!(
            "Persistent Encoding of Update #{}: {}ms",
            i, update_encoding_time,
        );
        write_encoding_time(path.clone(), update_encoding_time);
        start = Instant::now();

        update_paths.push((update_path, a.to_owned(), b.to_owned()));
    }

    let safe_encoder = Arc::new(encoder);

    timely::execute_from_args(timely_params, move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = timely::dataflow::ProbeHandle::new();

        // VERY IMPORTANT:
        // TBox data needs to be inserted by EACH WORKER, hence we don't pass the
        // index and the peers to parallelize the computation.
        let t_data = E::load_encoded_from_persistent(t_box_encoded_path.clone(), None, None);
        let a_data =
            E::load_encoded_from_persistent(a_box_encoded_path.clone(), Some(index), Some(peers));

        let load_time = timer.elapsed().as_millis();
        info!("Worker {}\t Load time: {}ms", index, load_time,);
        timer = std::time::Instant::now();

        let (mut data_input, mut result_trace) = worker.dataflow::<usize, _, _>(|scope| {
            let (data_input, data_collection) =
                scope.new_collection::<<E::EncodedDataSet as IntoIterator>::Item, _>();
            let res_trace = materialization(&data_collection, &mut probe, &rdfs_keywords);
            (data_input, res_trace)
        });

        insert_starting_data::<E, _, _>(a_data, &mut data_input, t_data);

        while probe.less_than(data_input.time()) {
            worker.step();
        }

        let full_mat_time = timer.elapsed().as_millis();
        info!(
            "Worker {}\t Full Materialization time: {}ms",
            index, full_mat_time,
        );
        timer = std::time::Instant::now();

        let mut output = args.output_folder.clone();
        output.push(format!("full_materialization_worker{}.nt", index));

        // This is basically not parallel since it locks the encoder during the execution of the
        // function
        save_to_file_through_trace::<E, _, _, _>(
            &safe_encoder.get_map().as_ref().unwrap(),
            output.as_path(),
            &mut result_trace,
            1,
        );

        let save_persistent_time = timer.elapsed().as_millis();
        info!(
            "Worker {}\t Saving to file time [Full Materialization]: {}ms",
            index, save_persistent_time,
        );
        timer = Instant::now();

        let full_mat_stats = Statistics {
            load_time,
            mat_time: full_mat_time,
            save_persistent_time,
        };

        full_mat_stats.write_to_file(args.output_folder.clone(), Some(index), Some(peers));

        // There is a `big` limit here. Dataflow Computation works great for applications where
        // all the data are the same. S(ame)IMD, sort of speaking. In our case we have T-Box triples that are different
        // from A-Bpx triples as each worker requires it. The current solution inserts all the
        // t-box triples for each worker. In cases where the TBox is really big this can
        // definitely be inefficient as the performance impact from multithreading is basically
        // very little. If only a_box is inserted than there should be no problem, I remember this
        // being an assumption that we made but DynamiTE doesn't really consider it.
        // [IMPROVEMENT]
        // What about a data structure shared by all the workers where all the t-box triples are
        // stored only once?

        for (i, (path, mode, t)) in update_paths.clone().iter().enumerate() {
            let data = match t {
                IncrementalType::TBox => E::load_encoded_from_persistent(path.clone(), None, None),
                IncrementalType::ABox => {
                    E::load_encoded_from_persistent(path, Some(index), Some(peers))
                }
            };
            let load_time = timer.elapsed().as_millis();
            info!(
                "Worker {}\t Update #{} Load time: {}ms",
                index,
                i + 1,
                load_time,
            );
            timer = std::time::Instant::now();

            match mode {
                IncrementalMode::Addition => add_data::<E, _, _>(data, &mut data_input, 2 + i),
                IncrementalMode::Deletion => remove_data::<E, _, _>(data, &mut data_input, 2 + i),
            }

            while probe.less_than(data_input.time()) {
                worker.step();
            }
            let mat_time = timer.elapsed().as_millis();
            info!(
                "Worker {}\t Update #{} Update time: {}ms",
                index,
                i + 1,
                mat_time,
            );

            timer = std::time::Instant::now();

            let mut changed_path = args.output_folder.to_owned();
            changed_path
                .push(&format!("incremental_materialization_{}_worker{}.nt", i + 1, index)[..]);

            save_to_file_through_trace::<E, _, _, _>(
                &safe_encoder.get_map().as_ref().unwrap(),
                changed_path,
                &mut result_trace,
                2 + i,
            );
            let save_persistent_time = timer.elapsed().as_millis();
            info!(
                "Worker {}\t Update #{} Save to File Time: {}ms",
                index,
                i + 1,
                save_persistent_time,
            );
            timer = std::time::Instant::now();

            let increm_stats = Statistics {
                load_time,
                mat_time,
                save_persistent_time,
            };

            let update_stats_path = std::path::PathBuf::from(path);
            let filename = update_stats_path.file_name().unwrap().to_str().unwrap();
            let len = filename.len();
            let subfilename = &filename[0..len - 14];
            let mut out = args.output_folder.clone();
            out.push("update_stats/");
            out.push(format!("{}_stats/", subfilename));
            increm_stats.write_to_file(out, Some(index), Some(peers))
        }
    })?
    // Main thread waits for all the workers to finish job so this guarantees that the evaluation
    // has been written and the main function can proceed an process them.
    .join();

    // Print evaluation of time spent per worker
    let mut stats_folder = another_args.output_folder.clone();
    stats_folder.push("stats/");
    crate::eval::output_figures(stats_folder);

    for path in another_args.incremental_file_paths.clone() {
        let folder = get_folder(&path.0, &another_args.output_folder);
        crate::eval::output_figures(folder);
    }

    Ok(())
}

fn get_folder(buf: &std::path::PathBuf, output: &std::path::PathBuf) -> std::path::PathBuf {
    let mut result = output.clone();
    let filename = buf.file_name().unwrap().to_str().unwrap();
    let index = filename.find('.').expect("Wrong format name of folder");
    result.push("update_stats/");
    result.push(format!("{}_stats/", &filename[0..index]));
    result.push("stats/");
    result
}

struct TimelyParams {
    params: Vec<String>,
}

impl Iterator for TimelyParams {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        if self.params.len() > 0 {
            Some(self.params.remove(0))
        } else {
            None
        }
    }
}

fn get_timely_args(args: std::sync::Arc<Args>) -> Vec<String> {
    let mut result = vec![];
    result.push("useless".to_string());
    if let Some(w) = args.workers {
        result.push("-w".to_string());
        result.push(format!("{}", w));
    }
    if let Some(n) = args.number_of_processes {
        result.push("-n".to_string());
        result.push(format!("{}", n));
    }
    if let Some(h) = args.hostfile.as_ref() {
        result.push("-h".to_string());
        result.push(format!("{:?}", h));
    }
    if let Some(p) = args.process_id {
        result.push("-p".to_string());
        result.push(format!("{}", p));
    }
    result
}
/// insert data provided by the abox or tbox into the dataflow through
/// the input handles.
pub fn insert_starting_data<E, K, V>(
    a_box: E::EncodedDataSet,
    data_input: &mut InputSession<
        usize,
        <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item,
        isize,
    >,
    t_box: E::EncodedDataSet,
) where
    E: EncoderTrait<K, V>,
    E::EncodedDataSet: std::iter::IntoIterator,
    <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item:
        std::fmt::Debug + Clone + Ord + 'static,
    // [IMPROVEMENT]:
    // Try to understand why V has to be 'static and think of the impact that
    // a static V has on performance.
    V: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
    K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
{
    for triple in t_box {
        data_input.insert(triple);
    }
    for triple in a_box {
        data_input.insert(triple);
    }

    // initial data are inserted all with timestamp 0, so we advance at time 1 and schedule the worker
    data_input.advance_to(1);
    data_input.flush();
}

pub fn add_data<E, K, V>(
    batch: E::EncodedDataSet,
    data_input: &mut InputSession<
        usize,
        <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item,
        isize,
    >,
    time_to_advance_to: usize,
) where
    E: EncoderTrait<K, V>,
    E::EncodedDataSet: std::iter::IntoIterator,
    <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item:
        std::fmt::Debug + Clone + Ord + 'static,
    // [IMPROVEMENT]:
    // Try to understand why V has to be 'static and think of the impact that
    // a static V has on performance.
    V: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
    K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
{
    for triple in batch {
        data_input.insert(triple);
    }

    data_input.advance_to(time_to_advance_to);
    data_input.flush();
}

pub fn remove_data<E, K, V>(
    batch: E::EncodedDataSet,
    data_input: &mut InputSession<
        usize,
        <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item,
        isize,
    >,
    time_to_advance_to: usize,
) where
    E: EncoderTrait<K, V>,
    E::EncodedDataSet: std::iter::IntoIterator,
    <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item:
        std::fmt::Debug + Clone + Ord + 'static,
    // [IMPROVEMENT]:
    // Try to understand why V has to be 'static and think of the impact that
    // a static V has on performance.
    V: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
    K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
{
    for triple in batch {
        data_input.remove(triple);
    }

    data_input.advance_to(time_to_advance_to);
    data_input.flush();
}

/// Save the full materialization fo file
pub fn save_to_file_through_trace<E, K, V, W: AsRef<std::path::Path>>(
    map: &E::MapStructure,
    path: W,
    trace: &mut TraceAgent<
        OrdKeySpine<
            <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item,
            usize,
            isize,
        >,
    >,
    time: usize,
) where
    E: EncoderTrait<K, V>,
    E::EncodedDataSet: std::iter::IntoIterator,
    <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item:
        std::fmt::Debug + Clone + Ord + 'static + Triple<V>,
    // [IMPROVEMENT]:
    // Try to understand why V has to be 'static and think of the impact that
    // a static V has on performance.
    V: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
    K: std::cmp::Eq + std::hash::Hash + std::fmt::Display + std::fmt::Debug,
{
    let mut full_materialization_file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        // Instead of expecting return a Result<()>
        .expect("Something wrong happened with the ouput file");

    if let Some((mut cursor, storage)) = trace.cursor_through(&[time]) {
        while let Some(key) = cursor.get_key(&storage) {
            while let Some(&()) = cursor.get_val(&storage) {
                let mut count = 0;
                cursor.map_times(&storage, |t, diff| {
                    // println!("{}, DIFF:{:?} ", key, diff);
                    if t.less_equal(&(time - 1)) {
                        count += diff;
                    }
                });
                if count > 0 {
                    // println!("{:?}", key);
                    // key.print_easy_reading();
                    // [IMPROVEMENT]:
                    // Error handling instead of unwraps and expects!
                    let s = map.get_left(key.s()).expect("Could not find the subject");
                    let p = map.get_left(key.p()).expect("Could not find the property");
                    let o = map.get_left(key.o()).expect("Could not find the object");
                    if let Err(e) = writeln!(full_materialization_file, "{} {} {} .", s, p, o) {
                        panic!("Couldn't write to file: {}", e);
                    }
                }
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage);
        }
    } else {
        println!("COULDN'T GET CURSOR");
    }
}

/// Saves the fragment of the materialization related to a worker in a vector so that it can be joined to create
/// the full file. TODO: IS THIS A LITTLE EXPENSIVE
pub fn return_vector<E, K, V>(
    trace: &mut TraceAgent<
        OrdKeySpine<
            <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item,
            usize,
            isize,
        >,
    >,
    time: usize,
) -> Vec<<<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item>
where
    E: EncoderTrait<K, V>,
    E::EncodedDataSet: std::iter::IntoIterator,
    <<E as encoder::EncoderTrait<K, V>>::EncodedDataSet as std::iter::IntoIterator>::Item:
        std::fmt::Debug + Clone + Ord + 'static,
    // [IMPROVEMENT]:
    // Try to understand why V has to be 'static and think of the impact that
    // a static V has on performance.
    V: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
    K: std::cmp::Eq + std::hash::Hash + std::fmt::Debug,
{
    let mut res = Vec::new();

    if let Some((mut cursor, storage)) = trace.cursor_through(&[time]) {
        while let Some(key) = cursor.get_key(&storage) {
            while let Some(&()) = cursor.get_val(&storage) {
                let mut count = 0;
                cursor.map_times(&storage, |t, diff| {
                    // println!("{}, DIFF:{:?} ", key, diff);
                    if t.less_equal(&(time - 1)) {
                        count += diff;
                    }
                });
                if count > 0 {
                    // println!("{:?}", key);
                    // key.print_easy_reading();
                    res.push(key.clone());
                }
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage);
        }
    } else {
        println!("COULDN'T GET CURSOR");
    }

    res
}

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn does_it() {
        assert!(true);
        println!("Showing");
    }
}
