use std::time::Duration;
use testcontainers::{
    GenericImage, ImageExt, core::IntoContainerPort, core::WaitFor, runners::AsyncRunner,
};

const SEPP_GRPC_PORT: u16 = 50051;

/// A Sepp server for the tests to talk to.
///
/// When `SEPP_TEST_ADDR` is set (e.g. `http://127.0.0.1:50051`), the tests run
/// against that server instead of starting a container — note they then share
/// one server and its state. Otherwise a fresh `ghcr.io/sepp-org/sepp:master`
/// testcontainer is started per test.
///
/// Panics when no server can be provided: a missing container runtime must
/// fail the integration suite loudly, never let it pass vacuously.
pub async fn start_sepp() -> SeppServer {
    if let Ok(addr) = std::env::var("SEPP_TEST_ADDR") {
        return SeppServer {
            _container: None,
            endpoint: addr,
        };
    }

    let container = GenericImage::new("ghcr.io/sepp-org/sepp", "latest")
        .with_exposed_port(SEPP_GRPC_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("queue server listening"))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect(
            "could not start the Sepp testcontainer (is a container runtime available?); \
             set SEPP_TEST_ADDR to use an already-running server instead",
        );

    let port = container
        .get_host_port_ipv4(SEPP_GRPC_PORT)
        .await
        .expect("could not get the Sepp container's mapped gRPC port");

    SeppServer {
        _container: Some(container),
        endpoint: format!("http://127.0.0.1:{port}"),
    }
}

pub struct SeppServer {
    // Held to keep the testcontainer alive; `None` when `SEPP_TEST_ADDR`
    // points at an externally managed server.
    _container: Option<testcontainers::ContainerAsync<GenericImage>>,
    endpoint: String,
}

impl SeppServer {
    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }
}
