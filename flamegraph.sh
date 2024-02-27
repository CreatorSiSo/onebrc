cd bench &&
cargo flamegraph --profile bench -- ../data/measurements.txt &&
hotspot perf.data
