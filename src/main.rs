#![feature(slice_split_once)]

use bstr::{BStr, ByteSlice};
use fxhash::FxHashMap;
use itertools::Itertools;
use kanal::Receiver;
use std::cmp::min;
use std::fs::File;
use std::sync::OnceLock;
use std::thread;

const NUM_THREADS: usize = 16;
const MAX_CHUNK: usize = 16 * 1024 * 1024;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    let file = File::open(path).unwrap();
    static MMAP: OnceLock<memmap2::Mmap> = OnceLock::new();
    // (UN)SAFETY: If someone else modifies the file while we are reading it, we are just fucked
    let mmap = MMAP.get_or_init(|| unsafe { memmap2::Mmap::map(&file) }.unwrap());

    let (sender, receiver) = kanal::unbounded::<&[u8]>();
    let threads = (0..NUM_THREADS)
        .map(|_| {
            let receiver = receiver.clone();
            thread::spawn(move || {
                let result = process_chunks(receiver);
                println!("Exiting thread");
                result
            })
        })
        .collect_vec();

    let mut start = 0;
    while start < mmap.len() {
        let chunk = &mmap[start..min(start + MAX_CHUNK, mmap.len())];
        let (lines, rest) = chunk.rsplit_once(|&b| b == b'\n').unwrap();
        sender.send(lines).unwrap();
        start += MAX_CHUNK - rest.len();
    }
    drop(sender);

    println!("Finished reading");

    let cities = threads
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .reduce(|mut accum, other| {
            for (city, temps) in other {
                if let Some(all_temps) = accum.get_mut(&city) {
                    all_temps.extend(temps);
                } else {
                    accum.insert(city, temps);
                }
            }
            accum
        })
        .unwrap_or_default();
    let out = cities
        .iter()
        .map(|(city, temps)| format_entry(city.as_bstr(), temps))
        .join(", ");

    println!("{{{out}}}");
    println!("{}", cities.len());
}

fn process_chunks(receiver: Receiver<&[u8]>) -> FxHashMap<&[u8], Vec<f32>> {
    let mut cities = FxHashMap::default();
    while let Ok(chunk) = receiver.recv() {
        for line in chunk.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let (city, temp_bytes) = line.split_once(|b| *b == b';').unwrap();
            let temp = fast_float::parse(temp_bytes).unwrap();
            cities
                .entry(city)
                .and_modify(|temps: &mut Vec<f32>| temps.push(temp))
                .or_insert(vec![temp]);
        }
    }
    cities
}

fn format_entry(city: &BStr, temps: &[f32]) -> String {
    let min = temps.iter().min_by(|a, b| a.total_cmp(b)).unwrap();
    let max = temps.iter().max_by(|a, b| a.total_cmp(b)).unwrap();
    let mean = match temps.len() {
        1 => temps[0],
        len if len % 2 == 0 => temps[len / 2],
        len => (temps[len / 2] + temps[len / 2 + 1]) / 2.0,
    };
    format!("{city}={min}/{mean}/{max}")
}
