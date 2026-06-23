use std::time::Duration;
use testcontainers::{core::IntoContainerPort, core::WaitFor, runners::AsyncRunner, GenericImage, ImageExt};

const SEPP_GRPC_PORT: u16 = 50051;

pub async fn start_sepp() -> Option<SeppContainer> {
    let container = match GenericImage::new("ghcr.io/sepp-org/sepp", "master")
        .with_exposed_port(SEPP_GRPC_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("queue server listening"))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: could not start Sepp container ({e})");
            return None;
        }
    };

    let port = match container.get_host_port_ipv4(SEPP_GRPC_PORT).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("skipping: could not get Sepp container port ({e})");
            return None;
        }
    };

    Some(SeppContainer {
        _container: container,
        port,
    })
}

pub struct SeppContainer {
    #[allow(dead_code)]
    _container: testcontainers::ContainerAsync<GenericImage>,
    pub port: u16,
}

impl SeppContainer {
    pub fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}
