fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    // Proto files are vendored into `proto/` (committed to the repo, shipped in
    // the published crate). The version below is the one currently vendored; to
    // refresh, bump it to a newer published label/commit and re-run:
    //
    //     buf export buf.build/sepp-org/sepp-proto:v1.1.0 -o proto
    //
    // The build itself never invokes `buf` or touches the network, so it works
    // on docs.rs, in offline CI, and for anyone who `cargo add`s this crate.
    let includes = &["proto"];
    let protos = &["proto/sepp/v1/queue.proto"];

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }
    println!("cargo:rerun-if-changed=proto/buf/validate/validate.proto");

    let mut prost_config = prost_build::Config::new();

    prost_reflect_build::Builder::new()
        .descriptor_pool("crate::pb::DESCRIPTOR_POOL")
        .configure(&mut prost_config, protos, includes)?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_with_config(prost_config, protos, includes)?;

    Ok(())
}
