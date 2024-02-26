#![feature(iter_array_chunks)]

use bus::{Bus, BusReader};
use itertools::Itertools;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::thread;

const NUM_THREADS: usize = 16;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    let mut channel = Bus::new(32);
    let mut threads = Vec::with_capacity(NUM_THREADS);
    for i in 0..NUM_THREADS {
        let receiver = channel.add_rx();
        threads.push(thread::spawn(move || {
            let result = task(receiver);
            println!("Exiting thread {i}");
            result
        }));
    }

    let file = File::open(path).unwrap();
    let lines = BufReader::new(file).lines().flatten().chunks(100_000);
    while let Some(chunk) = lines.into_iter().next() {
        channel.broadcast(chunk.collect::<Vec<String>>());
    }
    drop(channel);

    println!("Finished reading");

    let mut all_cities: HashMap<String, Vec<f32>> = HashMap::new();
    for handle in threads {
        let cities = handle.join().unwrap();
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

fn task(mut receiver: BusReader<Vec<String>>) -> HashMap<String, Vec<f32>> {
    let mut cities = HashMap::new();
    while let Ok(chunk) = receiver.recv() {
        for line in chunk {
            let (city, temp_str) = line.split_once(';').unwrap();
            let temp = temp_str.parse().unwrap();
            cities
                .entry(city.to_owned())
                .and_modify(|temps: &mut Vec<f32>| temps.push(temp))
                .or_insert(vec![temp]);
        }
    }
    cities
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
