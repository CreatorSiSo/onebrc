#![feature(slice_split_once)]

use itertools::Itertools;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use tokio::task::JoinSet;

const NUM_THREADS: usize = 16;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    let (sender, receiver) = kanal::unbounded_async::<Vec<u8>>();
    let mut tasks = JoinSet::new();
    for i in 0..NUM_THREADS {
        let receiver = receiver.clone();
        tasks.spawn(async move {
            let mut cities = HashMap::new();
            while let Ok(chunk) = receiver.recv().await {
                process_chunk(chunk, &mut cities)
            }
            println!("Exiting task {i}");
            cities
        });
    }

    let file = File::open(path).unwrap();
    let mut reader = BufReader::new(file);
    let mut chunk = [0; 1_000_000];
    loop {
        let len_read = reader.read(&mut chunk).unwrap();
        if len_read == 0 {
            break;
        }
        let (lines, rest) = chunk[..len_read].rsplit_once(|&b| b == b'\n').unwrap();
        sender.send(lines.to_owned()).await.unwrap();
        reader.seek_relative(-(rest.len() as i64)).unwrap();
    }
    drop(sender);

    println!("Finished reading");

    let mut all_cities: HashMap<String, Vec<f32>> = HashMap::new();
    while let Some(Ok(cities)) = tasks.join_next().await {
        for (city, temps) in cities {
            if let Some(all_temps) = all_cities.get_mut(&city) {
                all_temps.extend(temps);
            } else {
                all_cities.insert(city, temps);
            }
        }
    }

    let out = all_cities
        .iter()
        .map(|(city, temps)| format_entry(city, temps))
        .join(", ");

    println!("{{{out}}}");
    println!("{}", all_cities.len());
}

fn process_chunk(chunk: Vec<u8>, cities: &mut HashMap<String, Vec<f32>>) {
    for line in chunk.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let (city, temp_str) = line.split_once(|b| *b == b';').unwrap();
        let temp = String::from_utf8_lossy(temp_str).parse().unwrap();
        cities
            .entry(String::from_utf8_lossy(city).to_string())
            .and_modify(|temps: &mut Vec<f32>| temps.push(temp))
            .or_insert(vec![temp]);
    }
}

fn format_entry(city: &str, temps: &[f32]) -> String {
    let min = temps.iter().min_by(|a, b| a.total_cmp(b)).unwrap();
    let max = temps.iter().max_by(|a, b| a.total_cmp(b)).unwrap();
    let mean = match temps.len() {
        1 => temps[0],
        len if len % 2 == 0 => temps[len / 2],
        len => (temps[len / 2] + temps[len / 2 + 1]) / 2.0,
    };
    format!("{city}={min}/{mean}/{max}")
}
