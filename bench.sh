cargo b --profile release &&
hyperfine "target/release/onebrc data/test-1.txt" --shell=none &&
hyperfine "target/release/onebrc data/test-10.txt" --shell=none &&
hyperfine "target/release/onebrc data/unique-1000.txt" &&
hyperfine "target/release/onebrc data/smaller.txt" &&
hyperfine "target/release/onebrc data/measurements.txt" &&
hyperfine "target/release/onebrc data/half.txt"
