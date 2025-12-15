#![feature(slice_split_once)]
#![feature(mpmc_channel)]

use rustc_hash::FxHashMap;
use std::cmp::min;
use std::fs::OpenOptions;
use std::num::NonZero;
use std::sync::mpmc::{Receiver, Sender};
use std::sync::OnceLock;
use std::thread;

struct Temperature {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

impl Temperature {
    fn new(temp: f32) -> Self {
        Self {
            min: temp,
            max: temp,
            sum: temp,
            count: 1,
        }
    }

    fn push(&mut self, temp: f32) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp;
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    // Open file in read-only mode
    let file = OpenOptions::new().read(true).open(path).unwrap();
    // TODO Lock file in shared read-only mode
    static MMAP: OnceLock<memmap2::Mmap> = OnceLock::new();
    // (UN)SAFETY: If someone else modifies the file while we are reading it, we are just fucked
    let mmap = MMAP.get_or_init(|| unsafe { memmap2::Mmap::map(&file) }.unwrap());

    let out = single_threaded(&mmap).fold(String::new(), |mut accum, (city, temps)| {
        if !accum.is_empty() {
            accum += ", ";
        }
        accum.push_str(&format_entry(city, &temps));
        accum
    });

    println!("{{{out}}}");
    // eprintln!("{}", cities.len());
}

fn single_threaded(data: &[u8]) -> impl Iterator<Item = (&[u8], Temperature)> {
    let mut map = FxHashMap::default();
    process_chunk(data, &mut map);
    map.into_iter()
}

fn multi_threaded<'a>(data: &'a [u8]) -> impl Iterator<Item = (&'a [u8], Temperature)> {
    const DEFAUL_NUM_THREADS: usize = 4;
    let num_threads = thread::available_parallelism()
        .map(NonZero::get)
        .unwrap_or(DEFAUL_NUM_THREADS);

    let (sender, receiver) = std::sync::mpmc::channel::<&[u8]>();

    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let receiver = receiver.clone();
                scope.spawn(move || process_chunks(receiver))
            })
            .collect();

        read_send_chunks(data, sender);

        handles
            .into_iter()
            .map(|handle| {
                let result = handle.join().unwrap();
                eprintln!("Joined thread");
                result
            })
            .reduce(|mut accum, other| {
                for (city, temps) in other {
                    accum
                        .entry(city)
                        .and_modify(|all_temps| all_temps.merge(&temps))
                        .or_insert(temps);
                }
                accum
            })
            .unwrap_or_default()
    })
    .into_iter()
}

fn read_send_chunks<'a>(data: &'a [u8], sender: Sender<&'a [u8]>) {
    const CHUNK_SIZE: usize = 16 * 1024 * 1024;

    let mut start = 0;
    while start < data.len() {
        let chunk = &data[start..min(start + CHUNK_SIZE, data.len())];
        let (lines, rest) = chunk.rsplit_once(|&b| b == b'\n').unwrap();
        sender.send(lines).unwrap();
        start += CHUNK_SIZE - rest.len();
    }

    eprintln!("Finished reading");
}

fn process_chunks(receiver: Receiver<&[u8]>) -> FxHashMap<&[u8], Temperature> {
    let mut map = FxHashMap::default();
    while let Ok(chunk) = receiver.recv() {
        process_chunk(chunk, &mut map);
    }
    map
}

fn process_chunk<'a>(chunk: &'a [u8], map: &mut FxHashMap<&'a [u8], Temperature>) {
    for line in chunk.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let (city, temp_bytes) = line.split_once(|b| *b == b';').unwrap();
        // SAFETY: Input is promised to be valid utf8
        let temp_str = unsafe { str::from_utf8_unchecked(temp_bytes) };
        let temp = temp_str.parse().unwrap();
        map.entry(city)
            .and_modify(|temps: &mut Temperature| temps.push(temp))
            .or_insert(Temperature::new(temp));
    }
}

fn format_entry(city: &[u8], temps: &Temperature) -> String {
    format!(
        "{}={}/{:.1}/{}",
        // SAFETY: Input is promised to be valid utf8
        unsafe { str::from_utf8_unchecked(city) },
        temps.min,
        temps.sum / temps.count as f32,
        temps.max
    )
}
