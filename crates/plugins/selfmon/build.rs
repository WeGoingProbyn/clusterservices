//! Compiles `proto/selfmon.proto`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const PROTO: &str = "proto/selfmon.proto";
    println!("cargo:rerun-if-changed={PROTO}");

    let descriptors = protox::compile([PROTO], ["proto"])?;
    prost_build::Config::new()
        // See cs-plugin-cgroup: keeps the plugin's dependency list to cs-api alone.
        .prost_path("cs_api::prost")
        .compile_fds(descriptors)?;
    Ok(())
}
