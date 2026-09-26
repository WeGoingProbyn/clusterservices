//! Compiles `proto/cgroup.proto`. See `cs-transport`'s build script: protox, so a
//! Rust toolchain is the only build requirement.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/cgroup.proto";
    println!("cargo:rerun-if-changed={PROTO}");

    let descriptors = protox::compile([PROTO], ["proto"])?;
    prost_build::Config::new()
        // Generate `cs_api::prost::...` rather than `::prost::...`, so this crate
        // needs no direct prost dependency and the rule holds as written: a plugin
        // depends on cs-api and nothing else. cs-api re-exports prost for exactly
        // this.
        .prost_path("cs_api::prost")
        .compile_fds(descriptors)?;
    Ok(())
}
