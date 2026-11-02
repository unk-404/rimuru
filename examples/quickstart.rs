use rimuru::{Endpoint, EndpointConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sender = Endpoint::bind("127.0.0.1:0".parse()?, EndpointConfig::default())?;
    let receiver = Endpoint::bind("127.0.0.1:0".parse()?, EndpointConfig::default())?;
    let session = sender.connect_async(receiver.local_addr()?).await?;

    let ticket = session.submit_reliable_async(b"Hello, Rimuru!").await?;
    let message = receiver.recv_async().await?;
    assert_eq!(message.as_slice(), b"Hello, Rimuru!");
    ticket.wait_async().await?;

    sender.close_async().await;
    receiver.close_async().await;
    Ok(())
}
