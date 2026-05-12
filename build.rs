use std::path::PathBuf;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    let remote_dir = out_dir.join("proto_remote");

    if remote_dir.exists() {
        std::fs::remove_dir_all(&remote_dir)?;
    }
    std::fs::create_dir_all(&remote_dir)?;

    for module in [
        "buf.build/sepp-org/sepp-proto",
        "buf.build/bufbuild/protovalidate",
    ] {
        let status = Command::new("buf")
            .args(["export", module, "-o"])
            .arg(&remote_dir)
            .status()?;
        if !status.success() {
            return Err(format!("buf export {module} failed").into());
        }
    }

    println!("cargo:rerun-if-changed=buf.lock");
    println!("cargo:rerun-if-changed=buf.yaml");

    let remote_dir_str = remote_dir.to_str().ok_or("non-utf8 OUT_DIR")?.to_string();
    let includes = &["proto", remote_dir_str.as_str()];
    let protos = &["queue.proto"];

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
