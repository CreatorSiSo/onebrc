use itertools::Itertools;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    let file = File::open(path).unwrap();
    let mut lines = BufReader::new(file).lines();

    let cities = std::thread::scope(|s| {
        let mut threads = vec![];
        for i in 0..=256 {
            let (sender, receiver) = std::sync::mpsc::channel::<String>();
            let task = move || {
                let mut cities = HashMap::new();
                while let Ok(line) = receiver.recv() {
                    let (city, temp_str) = line.split_once(';').unwrap();
                    let temp = temp_str.parse().unwrap();
                    cities
                        .entry(city.to_owned())
                        .and_modify(|temps: &mut Vec<f32>| temps.push(temp))
                        .or_insert(vec![temp]);
                }
                println!("Exiting thread {i}");
                cities
            };
            threads.push((sender, s.spawn(task)));
        }

        while let Some(Ok(line)) = lines.next() {
            let thread_index = line.as_bytes()[0];
            threads[thread_index as usize].0.send(line).unwrap();
        }

        println!("finished reading");

        let mut all_cities: HashMap<String, Vec<f32>> = HashMap::new();
        for (sender, handle) in threads {
            drop(sender);
            all_cities.extend(handle.join().unwrap());
        }

        all_cities
    });

    let out = cities
        .iter()
        .map(|(city, temps)| {
            let mut min = temps[0];
            let mut max = temps[0];
            let mean = match temps.len() {
                1 => temps[0],
                len if len % 2 == 0 => temps[len / 2],
                len => (temps[len / 2] + temps[len / 2 + 1]) / 2.0,
            };
            for temp in temps {
                if *temp < min {
                    min = *temp;
                }
                if *temp > max {
                    max = *temp;
                }
            }
            format!("{city}={min}/{mean}/{max}")
        })
        .join(", ");

    println!("{{{out}}}");
}
