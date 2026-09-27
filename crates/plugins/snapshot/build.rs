//! Compiles `proto/snapshot.proto`. See `cs-plugin-cgroup`'s build script.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/snapshot.proto";
    println!("cargo:rerun-if-changed={PROTO}");

    let descriptors = protox::compile([PROTO], ["proto"])?;
    prost_build::Config::new()
        .prost_path("cs_api::prost")
        .compile_fds(descriptors)?;
    Ok(())
}
