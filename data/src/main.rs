use rand::Rng;
use std::{env::args, fs::File, io::Write, num::NonZero};

const STATIONS: &str = include_str!("../weather_stations.csv");

fn main() {
    let mut args = args().skip(1);
    let dest = args.next().unwrap();
    let rows: usize = args.next().unwrap().parse().unwrap();
    let min_max_table = &*Box::leak(generate_min_max_table());

    let n_threads = std::thread::available_parallelism().unwrap_or(NonZero::new(1).unwrap());
    let rows_chunk = rows / n_threads.get();
    let rows_rest = rows - (rows_chunk * n_threads.get());

    let handles = (0..n_threads.get())
        .map(|_| std::thread::spawn(move || generate_data(min_max_table, rows_chunk)));

    let mut file = File::create(dest).unwrap();
    for handle in handles {
        let lines = handle.join().unwrap();
        file.write_all(lines.as_bytes()).unwrap();
    }

    for _ in 0..rows_rest {
        let lines = generate_data(min_max_table, rows_rest);
        file.write_all(lines.as_bytes()).unwrap();
    }
}

fn generate_min_max_table() -> Box<[(&'static str, f32, f32)]> {
    const MIN: f32 = -99.9;
    const MAX: f32 = 99.9;

    let mut rng = rand::rng();
    let mut entries = Vec::with_capacity(STATIONS.len());

    let mut lines = STATIONS
        .split('\n')
        .skip(2)
        .flat_map(|line| line.split_once(';'));
    while let Some((station, avg_temp)) = lines.next() {
        let avg_temp = avg_temp.parse().unwrap();
        let rand_min = rng.random_range(MIN..avg_temp);
        let rand_max = rng.random_range(avg_temp..=MAX);
        entries.push((station, rand_min, rand_max));
    }

    entries.into_boxed_slice()
}

fn generate_data(min_max_table: &[(&'static str, f32, f32)], rows: usize) -> String {
    let mut rng = rand::rng();
    let mut lines = String::new();

    for _ in 0..rows {
        let (station, min, max) = min_max_table[rng.random_range(0..min_max_table.len())];
        lines.push_str(station);
        lines.push(';');
        lines.push_str(&format!("{:.1}", rng.random_range(min..=max)));
        lines.push('\n');
    }

    lines
}
