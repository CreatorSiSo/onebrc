#![feature(slice_split_once)]
#![feature(mpmc_channel)]

use rustc_hash::FxHashMap;
use std::cmp::min;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::num::NonZero;
use std::os::raw::c_void;
use std::sync::mpmc::{Receiver, Sender};
use std::sync::OnceLock;
use std::thread;

fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();

    // Open file in read-only mode
    let file = OpenOptions::new().read(true).open(path).unwrap();
    // TODO Lock file in shared read-only mode
    static MMAP: OnceLock<memmap2::Mmap> = OnceLock::new();
    // (UN)SAFETY: If someone else modifies the file while we are reading it, we are just fucked
    let mmap = MMAP.get_or_init(|| unsafe { memmap2::Mmap::map(&file) }.unwrap());
    mmap.advise(memmap2::Advice::Sequential).unwrap();

    let sorted = BTreeMap::from_iter(single_threaded(&mmap));

    let out = sorted
        .into_iter()
        .fold(String::new(), |mut accum, (city, temps)| {
            if !accum.is_empty() {
                accum += ", ";
            }
            accum.push_str(&format_entry(&city, &temps));
            accum
        });

    println!("{{{out}}}");
    // eprintln!("{}", cities.len());
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

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Decimal(i16);

impl Decimal {
    /// bytes must be of length 3..=5 with the format
    ///
    /// decimal ::= "-"? digit? digit "." digit
    /// digit ::= "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"
    fn parse(bytes: &[u8]) -> Self {
        #[inline(always)]
        const fn digit_from_byte(byte: u8) -> i16 {
            (byte - b'0') as i16
        }

        let mut inner = 0i16;
        let index_last = bytes.len() - 1;

        let is_negative = (bytes[0] - b'-') == 0x00;

        inner += digit_from_byte(bytes[index_last]);
        inner += digit_from_byte(bytes[index_last - 2]) * 10;
        let has_tens = (bytes.len() - (is_negative as usize)) == 4;
        if has_tens {
            inner += digit_from_byte(bytes[index_last - 3]) * 100;
        }

        inner *= if is_negative { -1 } else { 1 };

        Self(inner)
    }

    fn as_f32(self) -> f32 {
        (self.0 as f32) / 10.0
    }
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
    let mut rest = chunk;

    while let Some(line) = next_line(&mut rest) {
        let (city, temp_bytes) = split_line(line);
        let temp = Decimal::parse(temp_bytes);
        map.entry(city)
            .and_modify(|temps: &mut Temperature| temps.push(temp))
            .or_insert(Temperature::new(temp));
    }
}

fn next_line<'a>(rest: &mut &'a [u8]) -> Option<&'a [u8]> {
    let line_end =
        unsafe { libc::memchr(rest.as_ptr() as *const c_void, b'\n' as i32, rest.len()) };
    if line_end.is_null() {
        return None;
    }

    let line_len = unsafe { line_end.byte_offset_from_unsigned(rest.as_ptr() as *mut u8) };
    let line = &rest[..line_len];
    if line.is_empty() {
        return None;
    }
    *rest = &rest[line_len + 1..];

    Some(line)
}

fn split_line(line: &[u8]) -> (&[u8], &[u8]) {
    let start = line.as_ptr() as *const c_void;

    // ignoring null, format specifies that there is always a semi on the line
    let split_ptr = unsafe { libc::memchr(start, b';' as i32, line.len()) };
    let split_pos = unsafe { split_ptr.byte_offset_from_unsigned(start as *mut u8) };

    (&line[..split_pos], &line[split_pos + 1..])
}

fn format_entry(city: &[u8], temps: &Temperature) -> String {
    format!(
        "{}={:.1}/{:.1}/{:.1}",
        // SAFETY: Input is promised to be valid utf8
        unsafe { str::from_utf8_unchecked(city) },
        temps.min.as_f32(),
        temps.sum / temps.count as f32,
        temps.max.as_f32()
    )
}
