#![feature(slice_split_once)]

use rustc_hash::{FxBuildHasher, FxHashMap};
use std::cmp::min;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::num::NonZero;
use std::os::raw::c_void;
use std::thread;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    // Open file in read-only mode
    let file = OpenOptions::new().read(true).open(path).unwrap();
    // (UN)SAFETY: If someone else modifies the file while we are reading it, we are just fucked
    let mmap = unsafe { memmap2::Mmap::map(&file) }.unwrap();
    // mmap.advise(memmap2::Advice::Sequential).unwrap();

    let stats = multi_threaded::do_work(&mmap);

    eprintln!("Collected stats");

    let out = stats
        .into_iter()
        .fold(String::new(), |mut accum, (city, temps)| {
            if !accum.is_empty() {
                accum += ", ";
            }
            accum.push_str(&format_entry(city, &temps));
            accum
        });

    println!("{{{out}}}");
}

struct Temperature {
    min: Decimal,
    max: Decimal,
    sum: f32,
    count: u32,
}

impl Temperature {
    fn new(temp: Decimal) -> Self {
        Self {
            min: temp,
            max: temp,
            sum: temp.as_f32(),
            count: 1,
        }
    }

    fn push(&mut self, temp: Decimal) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp.as_f32();
        self.count += 1;
    }

    fn merge(&mut self, other: &Self) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
    }
}

fn process_chunk(chunk: &[u8]) -> FxHashMap<&[u8], Temperature> {
    let mut stats = FxHashMap::with_capacity_and_hasher(1024, FxBuildHasher);
    let mut rest = chunk;

    loop {
        let line = next_line(&mut rest);
        if line.is_empty() {
            break;
        }
        let (city, temp_bytes) = split_line(line);
        let temp = Decimal::parse(temp_bytes);
        stats
            .entry(city)
            .and_modify(|temps: &mut Temperature| temps.push(temp))
            .or_insert(Temperature::new(temp));
    }

    stats
}

fn next_line<'a>(rest: &mut &'a [u8]) -> &'a [u8] {
    let start = rest.as_ptr() as *const c_void;
    let line_end = unsafe { libc::memchr(start, i32::from(b'\n'), rest.len()) };

    if line_end.is_null() {
        let line = &rest[..];
        *rest = &rest[rest.len()..];
        line
    } else {
        let line_len = unsafe { line_end.offset_from_unsigned(start) };
        let line = &rest[..line_len];
        *rest = &rest[line_len + 1..];
        line
    }
}

/// Second element in result contains semicolon at index 0
fn split_line(line: &[u8]) -> (&[u8], &[u8]) {
    let start = line.as_ptr() as *const c_void;

    // SAFETY: format specifies that there is always a semicolon on the line
    let split_ptr = unsafe { libc::memchr(start, i32::from(b';'), line.len()) };
    let split_pos = unsafe { split_ptr.offset_from_unsigned(start) };

    (&line[..split_pos], &line[split_pos..])
}

fn format_entry(city: String, temps: &Temperature) -> String {
    format!(
        "{}={:.1}/{:.1}/{:.1}",
        city,
        temps.min.as_f32(),
        temps.sum / temps.count as f32,
        temps.max.as_f32()
    )
}

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Decimal(i16);

impl Decimal {
    /// bytes must be of length 4..=6 (first byte is a semicolon and not part of the decimal)
    ///
    /// with the format:
    ///
    /// decimal ::= ";" "-"? digit? digit "." digit
    /// digit ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
    fn parse(bytes: &[u8]) -> Self {
        #[inline(always)]
        const fn digit_from_byte(byte: u8) -> i16 {
            (byte - b'0') as i16
        }

        assert!(bytes.len() >= 4);

        // ignore padding byte at index 0
        let first = 1;
        let last = bytes.len() - 1;

        let has_tens = !(bytes[last - 3] == b'-' || bytes[last - 3] == b';');

        let mut inner = 0i16;
        inner += digit_from_byte(bytes[last]);
        inner += digit_from_byte(bytes[last - 2]) * 10;
        inner += digit_from_byte(bytes[last - 3]) * (has_tens as i16) * 100;

        let sign = i16::from(bytes[first] != b'-') * 2 - 1;
        inner *= sign;

        // eprintln!("{} -> {}", str::from_utf8(bytes).unwrap(), inner);

        Self(inner)
    }

    fn as_f32(self) -> f32 {
        (self.0 as f32) / 10.0
    }
}

mod single_threaded {
    use super::*;

    pub fn do_work(data: &[u8]) -> BTreeMap<String, Temperature> {
        BTreeMap::from_iter(process_chunk(data).into_iter().map(|(city, temps)| {
            (
                // SAFETY: Input is promised to be valid utf8
                unsafe { String::from_utf8_unchecked(Vec::from(city)) },
                temps,
            )
        }))
    }
}

mod multi_threaded {
    use super::*;

    pub fn do_work<'a>(data: &'a [u8]) -> BTreeMap<String, Temperature> {
        let mut stats = BTreeMap::new();

        std::thread::scope(|scope| {
            let num_threads = thread::available_parallelism().map(NonZero::get).unwrap();
            eprintln!("Number of threads: {num_threads}");
            let chunks = create_chunks(data, num_threads);

            let (sender, receiver) = std::sync::mpsc::channel();
            for chunk in chunks {
                let sender = sender.clone();
                eprintln!("spawned");
                scope.spawn(move || sender.send(process_chunk(chunk)));
            }

            drop(sender);
            for stat in receiver {
                for (city, temps) in stat {
                    // SAFETY: Input is promised to be valid utf8
                    let name = unsafe { String::from_utf8_unchecked(Vec::from(city)) };
                    match stats.entry(name) {
                        Entry::Vacant(vacant) => {
                            vacant.insert(temps);
                        }
                        Entry::Occupied(occupied) => {
                            occupied.into_mut().merge(&temps);
                        }
                    };
                }
            }
        });

        stats
    }

    fn create_chunks(data: &[u8], num_threads: usize) -> Vec<&[u8]> {
        let chunk_size = data.len() / num_threads;
        let mut chunks = Vec::new();

        let mut start = 0;
        while start < data.len() {
            let chunk = &data[start..min(start + chunk_size, data.len())];
            let (lines, rest) = chunk.rsplit_once(|&b| b == b'\n').unwrap();
            chunks.push(lines);
            start += chunk_size - rest.len();
        }

        chunks
    }
}
