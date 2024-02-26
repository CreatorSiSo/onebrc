use itertools::Itertools;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    let file = File::open(path).unwrap();
    let mut lines = BufReader::new(file).lines();

    let mut cities: HashMap<String, Vec<f32>> = HashMap::new();

    while let Some(Ok(line)) = lines.next() {
        let (city, temp_str) = line.split_once(';').unwrap();
        let temp = temp_str.parse().unwrap();
        cities
            .entry(city.to_owned())
            .and_modify(|temps: &mut Vec<f32>| temps.push(temp))
            .or_insert(vec![temp]);
    }

    println!("finished reading");

    let out = cities
        .iter()
        .map(|(city, temps)| {
            let min = temps.iter().min_by(|a, b| a.total_cmp(b)).unwrap();
            let max = temps.iter().max_by(|a, b| a.total_cmp(b)).unwrap();
            let mean = match temps.len() {
                1 => temps[0],
                len if len % 2 == 0 => temps[len / 2],
                len => (temps[len / 2] + temps[len / 2 + 1]) / 2.0,
            };
            format!("{city}={min}/{mean}/{max}")
        })
        .join(", ");

    println!("{{{out}}}");
}
