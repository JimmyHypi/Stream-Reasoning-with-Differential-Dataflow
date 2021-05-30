use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug)]
/// Contains full materialization statistics
pub struct Statistics {
    /// load time
    pub load_time: u128,
    /// materialization time, just to record multiple runs to average out the full mat time
    pub mat_time: u128,
    /// full materialization time to save from trace to vec, to be joined in the main thread
    pub save_persistent_time: u128,
}

impl Statistics {
    /// Save statistics relative to worker to file
    pub fn write_to_file(&self, file_path: PathBuf, index: Option<usize>, peers: Option<usize>) {
        let mut path_buf = file_path.clone();
        let peers = peers.unwrap_or(1);
        let index = index.unwrap_or(0);

        path_buf.push(format!("stats/peers{}/", peers).as_str());
        std::fs::create_dir_all(path_buf.clone())
            .expect("Could not create `stats/peers/` directory");
        path_buf.push(format!("worker{}", index));

        let mut file = open_append(path_buf.clone());

        let metadata = std::fs::metadata(path_buf.clone()).expect("Could not get metadata");
        // If the file is empty it means that it's the first iteration, we want
        // to append new results to previous results so to have more data to analyze
        if metadata.len() == 0 {
            writeln!(file, "Load Time, Materialization, Save to File Time (ms)")
                .expect("Invalid File Path");
        }

        let string = format!(
            "{}, {}, {}",
            self.load_time, self.mat_time, self.save_persistent_time
        );
        writeln!(file, "{}", string).expect("Could not write statistics to file");
    }
}

use log::info;
use plotlib::page::Page;
use plotlib::repr::Plot;
use plotlib::style::{LineJoin, LineStyle, PointMarker, PointStyle};
use plotlib::view::ContinuousView;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use walkdir::DirEntry;
use walkdir::WalkDir;

// Assumption: all statistics are saved in the output/stats folder. The input is the output
// directory. The function will look for that specific folder.
pub fn output_figures(output_path: PathBuf) {
    // let stats_path =
    // locate_stats_folder(output_path.clone(), false).expect("Could not find stats folder");
    // println!("stats_path: {:?}", stats_path);
    let stats_path = output_path.clone();

    let mut load_time_per_peers: Vec<(f64, f64)> = vec![];
    let mut mat_time_per_peers: Vec<(f64, f64)> = vec![];
    let mut save_to_file_time_per_peers: Vec<(f64, f64)> = vec![];

    // these two variables per plot are needed to compute the range in the y axis. We save the
    // worker with min time needed for further plotting by the client (e.g., how load-mat-stf time
    // change per each university.
    let ((mut load_time_min_val, mut load_peers), mut load_time_max_val) = ((0., 0usize), 0.);
    let ((mut mat_time_min_val, mut mat_peers), mut mat_time_max_val) = ((0., 0usize), 0.);
    let ((mut save_to_file_time_min_val, mut sft_peers), mut save_to_file_time_max_val) =
        ((0., 0usize), 0.);

    // Each directory correspond to the computation data for a determined amount of workers.
    // The convention has the folder named peersX where X is the number of total workers for easy
    // parsing. So for each directory:
    for entry in WalkDir::new(output_path.clone()).min_depth(1).max_depth(1) {
        let entry = entry.expect("Failed to read file in stats path");
        if entry.path().is_dir() && is_peers_folder(&entry.path()) {
            // The convention calls the folder "peersX" so we need to skip "peers" to get to the number
            // of peers
            let peers_number = peers_from_file(&entry);

            // We consider the worst performing worker as the data to show, as we care about the worst
            // case scenario.
            let mut max_load_time = -1.;
            let mut max_mat_time = -1.;
            let mut max_save_to_file_time = -1.;

            // Iterate over all files inside peersX folder. Convention: these files are named workerX.
            // Where X is the number of the worker.
            // Read all the files (there should be as many files as X). For each set of peers
            for file in WalkDir::new(entry.path()).min_depth(1) {
                let file = file.expect("Could not traverse peers folder");
                let (avg_lt, avg_mt, avg_sft) = get_averaged_data(file.path());

                // Save the worst performant result
                if avg_lt.gt(&max_load_time) {
                    max_load_time = avg_lt
                }
                if avg_mt.gt(&max_mat_time) {
                    max_mat_time = avg_mt
                }
                if avg_sft.gt(&max_save_to_file_time) {
                    max_save_to_file_time = avg_sft
                }
            }

            if max_load_time.ge(&load_time_max_val) {
                // At the first iteration we need to initialize both min and max to the same value.
                // max_load_time will always be greater than 0 (load_time_max_val) in the first
                // iteration.
                if load_time_max_val == 0. {
                    load_time_min_val = max_load_time;
                    load_peers = peers_number;
                }
                load_time_max_val = max_load_time;
            } else if max_load_time.lt(&load_time_min_val) {
                load_time_min_val = max_load_time;
                load_peers = peers_number;
            }

            if max_mat_time.ge(&mat_time_max_val) {
                // At the first iteration we need to initialize both min and max to the same value.
                // max_load_time will always be greater than 0 (load_time_max_val) in the first
                // iteration.
                if mat_time_max_val == 0. {
                    mat_time_min_val = max_mat_time;
                    mat_peers = peers_number;
                }
                mat_time_max_val = max_mat_time;
            } else if max_mat_time.lt(&mat_time_min_val) {
                mat_time_min_val = max_mat_time;
                mat_peers = peers_number;
            }

            if max_save_to_file_time.ge(&save_to_file_time_max_val) {
                // At the first iteration we need to initialize both min and max to the same value.
                // max_load_time will always be greater than 0 (load_time_max_val) in the first
                // iteration.
                if save_to_file_time_max_val == 0. {
                    save_to_file_time_min_val = max_save_to_file_time;
                    sft_peers = peers_number;
                }
                save_to_file_time_max_val = max_save_to_file_time;
            } else if max_save_to_file_time.lt(&save_to_file_time_min_val) {
                save_to_file_time_min_val = max_save_to_file_time;
                sft_peers = peers_number;
            }

            // Average the value of all the three vectors
            load_time_per_peers.push((peers_number as f64, max_load_time));
            mat_time_per_peers.push((peers_number as f64, max_mat_time));
            save_to_file_time_per_peers.push((peers_number as f64, max_save_to_file_time));
        }
    }
    /*
    info!("({}, {})", load_time_min_val, load_time_max_val);
    info!("({}, {})", mat_time_min_val, mat_time_max_val);
    info!(
        "({}, {})",
        save_to_file_time_min_val, save_to_file_time_max_val
    );
    */
    info!(
        "Best Load Time: {}ms with {} workers",
        load_time_min_val, load_peers
    );
    info!(
        "Best Materialization Time: {}ms with {} workers",
        mat_time_min_val, mat_peers
    );
    info!(
        "Best Save-to-File Time: {}ms with {} workers",
        save_to_file_time_min_val, sft_peers
    );

    let load_best = (load_time_min_val, load_peers);
    let mat_best = (mat_time_min_val, mat_peers);
    let sft_best = (save_to_file_time_min_val, sft_peers);

    let mut best = stats_path.clone();
    best.push("best_results/");
    if !best.is_dir() {
        std::fs::create_dir_all(best.clone()).unwrap();
    }

    let mut load_best_path = best.clone();
    load_best_path.push("load_best.txt");
    let mut mat_best_path = best.clone();
    mat_best_path.push("materialization_best.txt");
    let mut sft_best_path = best.clone();
    sft_best_path.push("save_to_file_best.txt");

    write_best_results(load_best_path, load_best);
    write_best_results(mat_best_path, mat_best);
    write_best_results(sft_best_path, sft_best);

    load_time_per_peers
        .sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare with NaN"));
    mat_time_per_peers
        .sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare with NaN"));
    save_to_file_time_per_peers
        .sort_by(|(a, _), (b, _)| a.partial_cmp(b).expect("Tried to compare with NaN"));

    /*
    info!("{:#?}", load_time_per_peers);
    info!("{:#?}", mat_time_per_peers);
    info!("{:#?}", save_to_file_time_per_peers);
    */

    // Plot the averaged values in the y axis and the number of workers.

    let mut plotter = Plotter::new();

    let load_time_plot = plotter.generate_plot(load_time_per_peers);
    let mat_time_plot = plotter.generate_plot(mat_time_per_peers);
    let save_to_file_time_plot = plotter.generate_plot(save_to_file_time_per_peers);

    let load_y_range = compute_axis_range(load_time_min_val, load_time_max_val, 0.4);
    let mat_y_range = compute_axis_range(mat_time_min_val, mat_time_max_val, 0.4);
    let save_to_file_y_range =
        compute_axis_range(save_to_file_time_min_val, save_to_file_time_max_val, 0.4);

    let load_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(load_y_range),
        x_label: "Number of Workers",
        y_label: "Load Time (ms)",
    };
    let mat_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(mat_y_range),
        x_label: "Number of Workers",
        y_label: "Materialization Time (ms)",
    };
    let save_to_file_plot_info = PlotInfo {
        x_range: None,
        y_range: Some(save_to_file_y_range),
        x_label: "Number of Workers",
        y_label: "Save to File Time (ms)",
    };

    let mut figures = stats_path.clone();
    figures.push("figures/");

    if !figures.is_dir() {
        std::fs::create_dir_all(figures.clone()).unwrap();
    }
    let mut load_file_path = figures.clone();
    let mut mat_file_path = figures.clone();
    let mut save_file_path = figures.clone();

    load_file_path.push("load_time.svg");
    mat_file_path.push("mat_time.svg");
    save_file_path.push("save_time.svg");

    plotter.save_plot(load_file_path, vec![load_time_plot], load_plot_info);
    plotter.save_plot(mat_file_path, vec![mat_time_plot], mat_plot_info);
    plotter.save_plot(
        save_file_path,
        vec![save_to_file_time_plot],
        save_to_file_plot_info,
    )
}
fn is_peers_folder(entry: &Path) -> bool {
    let filename = entry.file_name().unwrap().to_str().unwrap();
    let peers = &filename[0..5];
    let number = &filename[5..];

    peers == "peers" && number.trim().parse::<usize>().is_ok()
}
fn write_best_results<P: AsRef<Path>>(path: P, res: (f64, usize)) {
    // The best Load Time, Materialization time and Save to File Time are written in three separate
    // files for an easier parsing from client
    let mut file = open_truncate(path);

    writeln!(file, "Number of Workers, Time (ms)\n{}, {}", res.1, res.0)
        .expect("Could not write best load to file");
}

pub fn open_truncate<P: AsRef<Path>>(path: P) -> std::fs::File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open(path)
        // Instead of expecting return a Result<()>
        .expect("Something wrong happened with the ouput file")
}
pub fn open_append<P: AsRef<Path> + std::fmt::Debug>(path: P) -> std::fs::File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .append(true)
        .create(true)
        .open(path)
        // Instead of expecting return a Result<()>
        .expect("Something wrong happened with the ouput file")
}
pub fn compute_axis_range(min: f64, max: f64, coeff: f64) -> (f64, f64) {
    if min == max {
        if max == 0. {
            return (-1., 1.);
        } else {
            return (min - coeff * max, max + coeff * max);
        }
    }
    (
        (min - coeff * (max - min)).floor(),
        (max + coeff * (max - min)).ceil(),
    )
}

fn peers_from_file(entry: &DirEntry) -> usize {
    // the convention calls the folder "peersx" so we need to skip "peers" to get to the number
    // of peers
    *&(entry
        .file_name()
        .to_str()
        .expect("could not convert osstr to string"))[5..]
        .parse::<usize>()
        .expect("could not parse to usize the number of peers")
}

fn get_averaged_data<W: AsRef<Path>>(path: W) -> (f64, f64, f64) {
    let mut load_acc = 0;
    let mut mat_acc = 0;
    let mut save_to_file_acc = 0;

    let mut count = 0;

    let file = std::fs::File::open(path).expect("Could not open data relative to a worker");

    let buf_read = BufReader::new(file);
    for line in buf_read.lines().skip(1) {
        let line = line.expect("Could not fetch line from data file");
        let mut iter = line.split(',');

        count += 1;

        load_acc += iter
            .next()
            .expect("Data format not valid")
            .trim()
            .parse::<u64>()
            .expect("Data format not valid");

        mat_acc += iter
            .next()
            .expect("Data format not valid")
            .trim()
            .parse::<u64>()
            .expect("Data format not valid");

        save_to_file_acc += iter
            .next()
            .expect("Data format not valid")
            .trim()
            .parse::<u64>()
            .expect("Data format not valid");

        assert!(iter.next().is_none());
    }
    (
        load_acc as f64 / count as f64,
        mat_acc as f64 / count as f64,
        save_to_file_acc as f64 / count as f64,
    )
}

/*
fn locate_stats_folder(output_path: PathBuf, update: bool) -> Result<PathBuf, String> {
    // Locate stats directory
    let mut stats_path = output_path.clone();
    // remove peers specific folder. TO BE CHANGED WHEN FIXED OUTPUT FOLDER CONVENTION
    // The update flags signals that this is an update and the performance data can be found
    // in the update_stats folder instead of stats folder
    if update {
        stats_path.push("update_stats/");
    } else {
        stats_path.push("stats/");
    }
    if stats_path.as_path().is_dir() {
        Ok(stats_path)
    } else {
        Err("Could not find stats file".to_string())
    }
}
*/

#[derive(Copy, Clone)]
pub struct PlotInfo<'a> {
    pub x_range: Option<(f64, f64)>,
    pub y_range: Option<(f64, f64)>,
    pub x_label: &'a str,
    pub y_label: &'a str,
}

impl<'a> PlotInfo<'a> {
    pub fn new() -> Self {
        Self {
            x_range: None,
            y_range: None,
            x_label: "hey",
            y_label: "yo",
        }
    }
}

pub struct Plotter {
    color_iter: Box<dyn Iterator<Item = Color>>,
}

impl Plotter {
    pub fn new() -> Self {
        Self {
            color_iter: Box::new(Color::iter()),
        }
    }
    // As of right now, PlotLib's Plot accepts only data as Vec<(f64, f64)>. So I am not able to
    // to make it generic. One idea is to make generic the Plotting Beckend in order to use
    // possibly different backends, but this is out of the scope right now.
    pub fn generate_plot(&mut self, data: Vec<(f64, f64)>) -> Plot {
        let current_color = self
            .color_iter
            .next()
            .expect("Did you really overlap or create more than 20 plots? Welp, then this needs to be a cyclic iterator or add mor colors yourself!");

        let line_style = LineStyle::new()
            .colour(current_color.get_hex())
            .width(1.0)
            .linejoin(LineJoin::Miter);

        let point_style = PointStyle::new()
            .marker(PointMarker::Circle)
            .colour(current_color.get_hex());

        Plot::new(data)
            .line_style(line_style)
            .point_style(point_style)
    }

    pub fn save_plot<P: AsRef<Path>>(&self, path: P, plots: Vec<Plot>, plot_info: PlotInfo) {
        let mut v = ContinuousView::new();

        for plot in plots {
            v = v.add(plot);
        }

        v = v.x_label(plot_info.x_label).y_label(plot_info.y_label); // .x_range(0.0, 6.0);
        if let Some((x1, x2)) = plot_info.x_range {
            v = v.x_range(x1, x2);
        }
        if let Some((y1, y2)) = plot_info.y_range {
            v = v.y_range(y1, y2);
        }
        Page::single(&v).save(path).expect("Could not generate svg");
    }
}

use strum::IntoEnumIterator;
use strum_macros::EnumIter;
// Overlapping plots are going to be generated when comparing different encoding logic
#[derive(Debug, EnumIter)]
enum Color {
    Maroon,
    Brown,
    Olive,
    Teal,
    Navy,
    Black,
    Red,
    Orange,
    Yellow,
    Lime,
    Green,
    Cyan,
    Blue,
    Purple,
    Magenta,
    Grey,
    Pink,
    Apricot,
    Beige,
    Mint,
    Lavender,
}

impl Color {
    fn get_hex(&self) -> &str {
        match self {
            Color::Maroon => "#800000",
            Color::Brown => "#9A6324",
            Color::Olive => "#808000",
            Color::Teal => "#469990",
            Color::Navy => "#000075",
            Color::Black => "#000000",
            Color::Red => "#E6194B",
            Color::Orange => "#F58231",
            Color::Yellow => "#FFE119",
            Color::Lime => "#BFEF45",
            Color::Green => "#3CB44B",
            Color::Cyan => "#42D4F4",
            Color::Blue => "#4363D8",
            Color::Purple => "#911EB4",
            Color::Magenta => "#F032E6",
            Color::Grey => "#A9A9A9",
            Color::Pink => "#FABED4",
            Color::Apricot => "#FFD8B1",
            Color::Beige => "#FFFAC8",
            Color::Mint => "#AAFFC3",
            Color::Lavender => "#DCBEFF",
        }
    }
}
