pub mod sepp {
    // prost-generated oneof variants trip this lint
    #[allow(clippy::enum_variant_names)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/sepp.v1.rs"));
    }
}
