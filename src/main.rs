use bus::Bus;
use itertools::Itertools;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};

const NUM_CORES: usize = 16;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    let file = File::open(path).unwrap();
    let mut reader = BufReader::new(file);

    let cities = std::thread::scope(|s| {
        let mut channel: Bus<Vec<u8>> = Bus::new(32);

        let mut threads = vec![];
        for i in 0..=NUM_CORES {
            let mut receiver = channel.add_rx();
            threads.push(s.spawn(move || {
                while let Ok(chunk) = receiver.recv() {
                    let mut cities = HashMap::new();
                    String::from_utf8_lossy(&chunk).lines().for_each(|line| {
                        let (city, temp_str) = line.split_once(';').unwrap();
                        let temp = temp_str.parse().unwrap();
                        cities
                            .entry(city.to_owned())
                            .and_modify(|temps: &mut Vec<f32>| temps.push(temp))
                            .or_insert(vec![temp]);
                    });
                }
                println!("Exiting thread {i}");
                // cities
            }));
        }

        let mut buf = [0; 4096 * 16];
        let mut start = 0;
        loop {
            let len = reader.read(&mut buf[start..]).unwrap();
            if len == 0 {
                break;
            }
            let mut data = &buf[..start + len];

            let overhang = data.rsplit(|byte| *byte == b'\n').next().unwrap();
            data = &data[..data.len() - overhang.len()];
            start = overhang.len();
            let range = data.len()..data.len() + overhang.len();

            channel.broadcast(data.to_vec());

            buf.copy_within(range, 0);
        }
        drop(channel);

        println!("finished reading");

        let mut all_cities: HashMap<String, Vec<f32>> = HashMap::new();
        for handle in threads {
            let cities = handle.join().unwrap();
            // for (city, temps) in cities {
            //     if let Some(all_temps) = all_cities.get_mut(&city) {
            //         all_temps.extend(temps);
            //     } else {
            //         all_cities.insert(city, temps);
            //     }
            // }
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
