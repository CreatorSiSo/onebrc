# One Billion Row Challenge

My implementation of the [One Billion Row Challenge](https://github.com/gunnarmorling/1brc) in Rust.

## Data Generation

TODO: Explain whats going on in `data/src/main.rs`

## Benchmarks

TODO: Write script that benchmarks 1..=64 threads

TODO: Add graph comparing to other solutions

## Dependencies

Yes this implementation has dependencies, which of this is technically not allowed ...
but I am also not writing this in Java and the competition is over anyways.

I am using `memmap2`/`libc` to access `mmap`, `madvise` and `memchr`,
which is fair imo because they just wrap standard functions.

`rustc_hash` ehhhh yeah, no excuses here really.
Just didn't feel like handrolling a hash function.
