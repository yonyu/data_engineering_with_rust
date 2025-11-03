use solana_client::{
    nonblocking::rpc_client::RpcClient,
    nonblocking::pubsub_client::PubsubClient,
    rpc_config::RpcBlockSubscribeFilter,
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let ws_url = "wss://api.mainnet-beta.solana.com/";
    let (mut client, receiver) = PubsubClient::block_subscribe(
        ws_url,
        RpcBlockSubscribeFilter::All,
        false,
    ).await?;

    while let Some(block_update) = receiver.next().await {
        // Serialize and write raw JSON to S3
        let raw_json = serde_json::to_string(&block_update)?;
        upload_to_s3("solana/bronze/", raw_json).await?;
    }

    client.shutdown().await;
    Ok(())
}

async fn upload_to_s3(prefix: &str, data: String) -> anyhow::Result<()> {
    use aws_sdk_s3::{Client, types::ByteStream};
    let client = Client::new(&aws_config::load_from_env().await);
    client.put_object()
        .bucket("solana-data-lake")
        .key(format!("{}{}.json", prefix, uuid::Uuid::new_v4()))
        .body(ByteStream::from(data.into_bytes()))
        .send()
        .await?;
    Ok(())
}
