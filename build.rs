fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    // Proto files are vendored into `proto/` (committed to the repo, shipped in
    // the published crate). To refresh them after an upstream change, run:
    //
    //     buf export buf.build/sepp-org/sepp-proto -o proto
    //
    // The build itself never invokes `buf` or touches the network, so it works
    // on docs.rs, in offline CI, and for anyone who `cargo add`s this crate.
    let includes = &["proto"];
    let protos = &["proto/queue.proto"];

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
