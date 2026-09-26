//! Compiles `proto/frame.proto`.
//!
//! Uses `protox` rather than `prost_build`'s default path so the build needs no
//! `protoc` binary — a Rust toolchain is the only requirement for building this
//! workspace, which matters on cluster build hosts.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/frame.proto";
    println!("cargo:rerun-if-changed={PROTO}");

    let descriptors = protox::compile([PROTO], ["proto"])?;

    prost_build::Config::new()
        // `bytes` fields become `Bytes`, so decoding a frame slices the input
        // buffer instead of copying every payload out of it.
        .bytes([".cs.frame.v1"])
        .compile_fds(descriptors)?;

    Ok(())
}
