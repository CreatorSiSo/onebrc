#![feature(slice_split_once)]
#![feature(exitcode_exit_method)]

use rustc_hash::{FxBuildHasher, FxHashMap};
use std::cmp::min;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Display;
use std::fs::OpenOptions;
use std::os::raw::c_void;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;
use std::thread;

struct Args {
    path: PathBuf,
    threads: Option<usize>,
}

impl Args {
    fn parse(mut args: impl Iterator<Item = String>) -> Option<Self> {
        // Skip executable path
        args.next()?;

        let mut result = Args {
            path: PathBuf::new(),
            threads: None,
        };

        let actual_args = args.collect::<Vec<_>>();
        let mut current_arg = 0;

        if let Some(threads_str) = actual_args.get(current_arg)
            && threads_str.starts_with("--threads")
        {
            let (_, number) = threads_str.split_once('=')?;
            result.threads = usize::from_str(number).ok();
            current_arg += 1;
        }

        result.path = PathBuf::from_str(actual_args.get(current_arg)?).ok()?;

        Some(result)
    }
}

fn main() {
    let args = match Args::parse(std::env::args()) {
        Some(args) => args,
        None => {
            eprintln!("Error parsing arguments");
            ExitCode::FAILURE.exit_process()
        }
    };

    // Open file in read-only mode
    let file = OpenOptions::new().read(true).open(args.path).unwrap();
    // (UN)SAFETY: If someone else modifies the file while we are reading it, we are just fucked
    let mmap = unsafe { memmap2::Mmap::map(&file) }.unwrap();
    // mmap.advise(memmap2::Advice::Sequential).unwrap();

    let num_threads = args
        .threads
        .unwrap_or_else(|| thread::available_parallelism().unwrap().get());
    let chunks = create_chunks(&mmap, num_threads);

    eprintln!("Configuration:");
    eprintln!("  Threads: {num_threads}");
    eprintln!(
        "  Chunks: [{}] ({})",
        chunks
            .iter()
            .map(|chunk| format!("{:.1}M", (chunk.len() as f64) / (1024 * 1024) as f64))
            .join(", "),
        chunks.len()
    );

    let mut statistics = BTreeMap::new();

    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        for chunk in chunks {
            let sender = sender.clone();
            scope.spawn(move || sender.send(process_chunk(chunk)));
        }
    });

    drop(sender);
    for stat in receiver {
        for (city, statistic) in stat {
            // SAFETY: Input is promised to be valid utf8
            let name = unsafe { str::from_utf8_unchecked(city) };
            match statistics.entry(name) {
                Entry::Vacant(vacant) => {
                    vacant.insert(statistic);
                }
                Entry::Occupied(occupied) => {
                    occupied.into_mut().merge(&statistic);
                }
            };
        }
    }

    let out = statistics
        .iter()
        .map(|(city, temps)| {
            format!(
                "{}={:.1}/{:.1}/{:.1}",
                city,
                temps.min.as_f32(),
                temps.sum / temps.count as f32,
                temps.max.as_f32()
            )
        })
        .join(",");
    println!("{{{out}}}");
}

fn create_chunks(data: &[u8], num_threads: usize) -> Vec<&[u8]> {
    let mut chunk_size = data.len() / num_threads;
    if data.len() < chunk_size {
        chunk_size = data.len();
    }
    let mut chunks = Vec::new();

    let mut start = 0;
    while start < data.len() {
        let chunk = &data[start..min(start + chunk_size, data.len())];

        let Some((lines, rest)) = chunk.rsplit_once(|&b| b == b'\n') else {
            unreachable!()
        };
        chunks.push(lines);
        start += chunk_size - rest.len();
    }

    chunks
}

fn process_chunk(chunk: &[u8]) -> FxHashMap<&[u8], Statistic> {
    let mut stats =
        FxHashMap::<_, Statistic>::with_capacity_and_hasher(10_000, FxBuildHasher::default());
    let mut rest = chunk;

    loop {
        let line = next_line(&mut rest);
        if line.is_empty() {
            break;
        }
        let (city, temp_bytes) = split_line(line);
        let temp = Decimal::parse(temp_bytes);
        if let Some(temps) = stats.get_mut(city) {
            temps.push(temp);
        } else {
            stats.insert(city, Statistic::new(temp));
        }
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
        let line = unsafe { &rest.get_unchecked(..line_len) };
        *rest = unsafe { &rest.get_unchecked(line_len + 1..) };
        line
    }
}

/// Second element in result contains semicolon at index 0
fn split_line(line: &[u8]) -> (&[u8], &[u8]) {
    let start = line.as_ptr() as *const c_void;

    // SAFETY: format specifies that there is always a semicolon on the line
    let split_ptr = unsafe { libc::memchr(start, i32::from(b';'), line.len()) };
    let split_pos = unsafe { split_ptr.offset_from_unsigned(start) };

    unsafe {
        (
            &line.get_unchecked(..split_pos),
            &line.get_unchecked(split_pos..),
        )
    }
}

struct Statistic {
    min: Decimal,
    max: Decimal,
    sum: f32,
    count: u32,
}

impl Statistic {
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

pub trait Join {
    type Item;

    /// Returns a string that contains the items separated by the separator string.
    /// ```rust
    /// assert_eq!((1..=5).join(", "), "1, 2, 3, 4, 5")
    /// ```
    fn join(&mut self, separator: impl Display) -> String
    where
        Self::Item: Display;
}

impl<I: Iterator> Join for I {
    type Item = I::Item;

    fn join(&mut self, separator: impl Display) -> String
    where
        Self::Item: Display,
    {
        self.fold(String::new(), |mut accum, item| {
            if !accum.is_empty() {
                accum += &separator.to_string()
            }
            accum + &item.to_string()
        })
    }
}
