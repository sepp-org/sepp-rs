fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = &["proto"];
    let protos = &["proto/sepp/v1/queue.proto"];

    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    let file_descriptors = protox::compile(protos, includes)?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_fds(file_descriptors)?;

    Ok(())
}
