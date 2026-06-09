use std::sync::LazyLock;

use prost_reflect::DescriptorPool;

pub static DESCRIPTOR_POOL: LazyLock<DescriptorPool> = LazyLock::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/file_descriptor_set.bin")).as_ref(),
    )
    .expect("failed to decode sepp file descriptor set")
});

pub mod sepp {
    // prost-generated oneof variants trip this lint
    #[allow(clippy::enum_variant_names)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/sepp.v1.rs"));
    }
}
