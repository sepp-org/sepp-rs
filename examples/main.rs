use std::collections::HashMap;

use sepp_rs::pb::sepp::v1::{
    EnqueueBatchRequest, EnqueueRequest, PrimitiveValue, primitive_value,
    queue_service_client::QueueServiceClient,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50051".into());
    let mut client = QueueServiceClient::connect(addr).await?;
    let info = client
        .enqueue_batch(EnqueueBatchRequest {
            jobs: vec![EnqueueRequest {
                queue: "123".to_string(),
                job_type: "asdf".to_string(),
                custom: HashMap::from([(
                    "int_value".to_string(),
                    PrimitiveValue {
                        value: Some(primitive_value::Value::IntValue(9_007_199_254_740_993)),
                    },
                )]),
                ..Default::default()
            }],
        })
        .await?
        .into_inner();

    //println!("{info:#?}");
    Ok(())
}
