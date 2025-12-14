#![feature(slice_split_once)]

use bstr::BStr;
use fxhash::FxHashMap;
use itertools::Itertools;
use kanal::{Receiver, Sender};
use std::cmp::min;
use std::fs::OpenOptions;
use std::num::NonZero;
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
        self.sum = self.sum + other.sum;
        self.count = self.count + other.count;
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    const DEFAUL_NUM_THREADS: usize = 4;
    let num_threads = thread::available_parallelism()
        .map(NonZero::get)
        .unwrap_or(DEFAUL_NUM_THREADS);

    let (sender, receiver) = kanal::unbounded::<&[u8]>();
    let handles = (0..num_threads)
        .map(|_| {
            let receiver = receiver.clone();
            thread::spawn(move || process_chunks(receiver))
        })
        .collect::<Vec<_>>();

    read_send_chunks(path, sender);

    let cities = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .reduce(|mut accum, other| {
            for (city, temps) in other {
                accum
                    .entry(&city)
                    .and_modify(|all_temps| all_temps.merge(&temps))
                    .or_insert(temps);
            }
            accum
        })
        .unwrap_or_default();
    let out = cities
        .iter()
        .map(|(city, temps)| format_entry(BStr::new(city), temps))
        .join(", ");

    println!("{{{out}}}");
    eprintln!("{}", cities.len());
}

fn read_send_chunks(path: String, sender: Sender<&[u8]>) {
    const CHUNK_SIZE: usize = 16 * 1024 * 1024;

    // Open file in read-only mode
    let file = OpenOptions::new().read(true).open(path).unwrap();
    // TODO Lock file in shared read-only mode
    static MMAP: OnceLock<memmap2::Mmap> = OnceLock::new();
    // (UN)SAFETY: If someone else modifies the file while we are reading it, we are just fucked
    let mmap = MMAP.get_or_init(|| unsafe { memmap2::Mmap::map(&file) }.unwrap());

    let mut start = 0;
    while start < mmap.len() {
        let chunk = &mmap[start..min(start + CHUNK_SIZE, mmap.len())];
        let (lines, rest) = chunk.rsplit_once(|&b| b == b'\n').unwrap();
        sender.send(lines).unwrap();
        start += CHUNK_SIZE - rest.len();
    }

    eprintln!("Finished reading");
}

fn process_chunks(receiver: Receiver<&[u8]>) -> FxHashMap<&[u8], Temperature> {
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
                .and_modify(|temps: &mut Temperature| temps.push(temp))
                .or_insert(Temperature::new(temp));
        }
    }

    eprintln!("Exiting thread");
    cities
}

fn format_entry(city: &BStr, temps: &Temperature) -> String {
    format!(
        "{city}={}/{:.1}/{}",
        temps.min,
        temps.sum / temps.count as f32,
        temps.max
    )
}
