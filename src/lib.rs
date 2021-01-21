//! Functions and definitions
#[macro_use]
extern crate lalrpop_util;

use crate::encoder::BiMapTrait;
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;
use differential_dataflow::trace::{cursor::Cursor, TraceReader};
use differential_dataflow::{Collection, ExchangeData};
use log::info;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
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
    workers: Option<usize>,
    #[structopt(short = "n", long = "processes")]
    number_of_processes: Option<usize>,
    #[structopt(short = "p", long = "process")]
    process_id: Option<usize>,
    #[structopt(short, long)]
    hostfile: Option<std::path::PathBuf>,
    #[structopt(parse(from_os_str))]
    t_box_path: std::path::PathBuf,
    #[structopt(parse(from_os_str))]
    a_box_path: std::path::PathBuf,
    #[structopt(parse(from_os_str))]
    output_folder: std::path::PathBuf,
    #[structopt(
        name = "UPDATE",
        short = "u",
        long = "update",
        parse(try_from_str = parse_key_val),
        number_of_values = 1,
    )]
    incremental_file_paths: Vec<(std::path::PathBuf, IncrementalMode, IncrementalType)>,
}

#[derive(Debug, Clone)]
enum IncrementalMode {
    Addition,
    Deletion,
}
#[derive(Debug, Clone)]
enum IncrementalType {
    ABox,
    TBox,
}
// [IMPROVEMENT]:
// Error Handling.
// This should go in its own module
#[derive(Debug)]
struct ParseModeError {
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
pub fn run_materialization<L, R, E, P, F, M>(
    encoder: Arc<Mutex<EncoderUnit<L, R, E, P, F>>>,
    materialization: M,
) -> Result<(), String>
where
    // [IMPROVEMENT]:
    // The timely dataflow constraint require all of the components that get passed from
    // worker to worker to be 'static because we don't want the closure to outlive
    // the variable.. Is there another way around?
    R: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + Send + Sync + 'static + Clone + Copy,
    L: std::cmp::Eq + std::hash::Hash + std::fmt::Debug + std::fmt::Display + Send + Sync + 'static,
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
        )
            -> TraceAgent<OrdKeySpine<<E::EncodedDataSet as IntoIterator>::Item, usize, isize>>
        + Send
        + Sync
        + 'static,
{
    let args = Arc::new(Mutex::new(Args::from_args()));
    let timely_args = get_timely_args(&args.lock().unwrap());
    let timely_params = TimelyParams {
        params: timely_args,
    };

    timely::execute_from_args(timely_params, move |worker| {
        let mut timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let mut probe = timely::dataflow::ProbeHandle::new();
        // VERY IMPORTANT:
        // TBox data needs to be inserted by EACH WORKER, hence we don't pass the
        // index and the peers to parallelize the computation.
        let t_data =
            encoder
                .lock()
                .unwrap()
                .encode(args.lock().unwrap().t_box_path.as_path(), None, None);
        let a_data = encoder.lock().unwrap().encode(
            args.lock().unwrap().a_box_path.as_path(),
            Some(index),
            Some(peers),
        );
        // [IMPROVEMENT]:
        // Here we are considering only the worker with index 0.. maybe an averatge among all the
        // workers?
        if index == 0 {
            info!("Load time: {}ms", timer.elapsed().as_millis());
            timer = std::time::Instant::now();
        }

        let (mut data_input, mut result_trace) = worker.dataflow::<usize, _, _>(|scope| {
            let (data_input, data_collection) =
                scope.new_collection::<<E::EncodedDataSet as IntoIterator>::Item, _>();
            let res_trace = materialization(&data_collection, &mut probe);
            (data_input, res_trace)
        });

        insert_starting_data::<E, _, _>(a_data, &mut data_input, t_data);

        while probe.less_than(data_input.time()) {
            worker.step();
        }

        if index == 0 {
            info!(
                "Full Materialization time: {}ms",
                timer.elapsed().as_millis()
            );
        }

        args.lock()
            .unwrap()
            .output_folder
            .push(format!("full_materialization_worker{}.nt", index));

        let mut locked = args.lock().unwrap();
        let output_path = locked.output_folder.to_owned();
        // Restore path
        locked.output_folder.pop();
        std::mem::drop(locked);

        save_to_file_through_trace::<E, _, _, _>(
            &encoder.lock().unwrap().get_map().as_ref().unwrap(),
            output_path.as_path(),
            &mut result_trace,
            1,
        );

        if index == 0 {
            info!(
                "Saving to file time [Full Materialization]: {}ms",
                timer.elapsed().as_millis(),
            );
        }

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
        let locked = args.lock().unwrap();
        let mut thread_safe = vec![];

        for (path, a, b) in locked.incremental_file_paths.iter() {
            thread_safe.push((path.to_owned(), a.to_owned(), b.to_owned()));
        }

        std::mem::drop(locked);

        for (i, (path, mode, t)) in thread_safe.iter().enumerate() {
            let data = match t {
                IncrementalType::TBox => encoder.lock().unwrap().encode(path.as_path(), None, None),
                IncrementalType::ABox => {
                    encoder
                        .lock()
                        .unwrap()
                        .encode(path.as_path(), Some(index), Some(peers))
                }
            };
            // [IMPROVEMENT]:
            // Here we are considering only the worker with index 0.. maybe an averatge among all the
            // workers?
            if index == 0 {
                info!(
                    "Update #{} Load time: {}ms",
                    i + 1,
                    timer.elapsed().as_millis()
                );
                timer = std::time::Instant::now();
            }

            match mode {
                IncrementalMode::Addition => add_data::<E, _, _>(data, &mut data_input, 2 + i),
                IncrementalMode::Deletion => remove_data::<E, _, _>(data, &mut data_input, 2 + i),
            }

            while probe.less_than(data_input.time()) {
                worker.step();
            }

            if index == 0 {
                info!(
                    "Update #{} Update Time: {}ms",
                    i + 1,
                    timer.elapsed().as_millis()
                );

                timer = std::time::Instant::now();
            }

            let locked = args.lock().unwrap();
            let mut path = locked.output_folder.to_owned();
            std::mem::drop(locked);
            path.push(&format!("incremental_materialization_{}_worker{}.nt", i + 1, index)[..]);

            save_to_file_through_trace::<E, _, _, _>(
                &encoder.lock().unwrap().get_map().as_ref().unwrap(),
                path,
                &mut result_trace,
                2 + i,
            );

            if index == 0 {
                info!(
                    "Update #{} Save to File Time: {}ms",
                    i + 1,
                    timer.elapsed().as_millis()
                );

                timer = std::time::Instant::now();
            }
        }
    })?;

    Ok(())
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

fn get_timely_args(args: &Args) -> Vec<String> {
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
                    if let Err(e) = writeln!(full_materialization_file, "<{}> <{}> <{}> .", s, p, o)
                    {
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

/*
/// Save the full materialization fo file
pub fn save_to_file_through_trace<E, K, V>(
    path: &str,
    trace: &mut TraceAgent<
        OrdKeySpine<
            <<E as encoder::Encoder<K, V>>::EncodedDataSet as std::iter::Iterator>::Item,
            usize,
            isize,
        >,
    >,
    time: usize,
) where
    E: Encoder<K, V>,
    E::EncodedDataSet: std::iter::Iterator,
    <<E as encoder::Encoder<K, V>>::EncodedDataSet as std::iter::Iterator>::Item:
        std::fmt::Debug + Clone + Ord + 'static,
    // [IMPROVEMENT]:
    // Try to understand why V has to be 'static and think of the impact that
    // a static V has on performance.
    V: std::cmp::Eq + std::hash::Hash,
    K: std::cmp::Eq + std::hash::Hash,
{

*/

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
/*
/// Concatenates the different fragments into the final file
pub fn save_concatenate(path: &str, result: Vec<Result<Vec<String>, String>>) {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut del_file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        .expect("Something wrong happened with the ouput file");
    // In case we already have it let's erase it and resave it instead of reconcatenating the whole thing
    // del_file.set_len(0).expect("Unable to reset file");

    for i in 0..result.len() {
        if let Some(Ok(vec)) = result.get(i) {
            for elem in vec {
                // println!("{}", elem);
                if let Err(e) = writeln!(del_file, "{}", elem) {
                    eprintln!("Couldn't write to file: {}", e);
                }
            }
        }
    }
}

/// outputs to file the results of the timely computation
pub fn save_stat_to_file(mat_path: &str, stat_path: &str, stats: Vec<model::Statistics>) {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut load_times: Vec<u128> = vec![];
    let mut mat_times: Vec<u128> = vec![];
    let mut mat_to_vec_times: Vec<u128> = vec![];

    let mut mat_file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open(mat_path)
        .expect("Something wrong happened with the ouput file");

    let save_time = std::time::Instant::now();
    for stat in &stats {
        if let Some(vec) = &stat.mat {
            for string in vec {
                // println!("{}", string);
                if let Err(e) = writeln!(mat_file, "{}", string) {
                    eprintln!("Couldn't write to file: {}", e);
                }
            }
        }
    }

    let time_to_save_mat = save_time.elapsed().as_nanos();

    for stat in stats {
        load_times.push(stat.load_time);
        mat_times.push(stat.mat_time);
        mat_to_vec_times.push(stat.mat_to_vec_time);
    }

    let load_time = load_times.iter().max().expect("No max found");
    let mat_time = mat_times.iter().max().expect("No max found");
    let mat_to_vec_time = mat_to_vec_times.iter().max().expect("No max found");
    // I don't believe this is ever going to overflow u128..
    let total_time_to_save = time_to_save_mat + mat_to_vec_time;


    let mut stat_file = OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open(stat_path)
        .expect("Something wrong happened with the ouput file");

    // Here we initialize the empty file with a directory saying what the columns are
    let metadata = std::fs::metadata(stat_path).expect("Could not get metadata");
    if metadata.len() == 0 {
        if let Err(e) = writeln!(stat_file, "Load Time,Materialization Time,Time to save to file") {
            eprintln!("Couldn't write to file: {}", e);
        }
    }

    println!("Load Time: {}", load_time);
    println!("Materialization Time: {}", mat_time);
    println!("Time to save to file: {}", total_time_to_save);

    let string = format!("{},{},{}", load_time, mat_time, total_time_to_save);
    if let Err(e) = writeln!(stat_file, "{}", string) {
        eprintln!("Couldn't write to file: {}", e);
    }
}
*/

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
