//! AMQP event publisher — publishes block.indexed events to RabbitMQ.
//!
//! Uses `amqprs` crate for async AMQP 0.9.1 communication.
//! If AMQP is not configured or connection fails, all publish calls
//! are silent no-ops (indexing never fails because of messaging).

use amqprs::{
    channel::{BasicPublishArguments, Channel, ExchangeDeclareArguments},
    connection::{Connection, OpenConnectionArguments},
    BasicProperties,
};
use tokio::sync::OnceCell;

/// The connection must outlive `init()`: dropping the last user-held
/// `Connection` handle makes amqprs send a close to the broker, which also
/// closes every channel opened on it. Keep it for the life of the process.
static CONNECTION: OnceCell<Connection> = OnceCell::const_new();
static CHANNEL: OnceCell<Channel> = OnceCell::const_new();
static EXCHANGE_NAME: OnceCell<String> = OnceCell::const_new();

/// Initialize the AMQP connection and declare the exchange.
/// Called once at indexer startup. If AMQP_URL env var is set, it
/// overrides the config file URL (allows SOPS secret injection).
///
/// On success returns a credential-free description of the broker endpoint
/// (host, port, vhost) suitable for logging.
pub async fn init(url: &str, exchange: &str) -> Result<String, String> {
    let effective_url = std::env::var("AMQP_URL").unwrap_or_else(|_| url.to_string());

    // Parse amqp://user:pass@host:port
    let parsed = url::Url::parse(&effective_url)
        .map_err(|e| format!("Invalid AMQP URL: {e}"))?;
    let host = parsed.host_str().unwrap_or("localhost");
    let port = parsed.port().unwrap_or(5672);
    let user = parsed.username();
    let password = parsed.password().unwrap_or("");

    let args = OpenConnectionArguments::new(host, port, user, password);
    // Never log the URL or `args`: both carry the password.
    let endpoint = format!("host={host} port={port} vhost=/");

    let connection = Connection::open(&args)
        .await
        .map_err(|e| format!("AMQP connect failed: {e}"))?;

    let channel = connection
        .open_channel(None)
        .await
        .map_err(|e| format!("AMQP channel failed: {e}"))?;

    channel
        .exchange_declare(ExchangeDeclareArguments::new(exchange, "topic").durable(true).finish())
        .await
        .map_err(|e| format!("AMQP exchange declare failed: {e}"))?;

    EXCHANGE_NAME
        .set(exchange.to_string())
        .map_err(|_| "AMQP exchange name already set".to_string())?;
    CONNECTION
        .set(connection)
        .map_err(|_| "AMQP connection already initialized".to_string())?;
    CHANNEL
        .set(channel)
        .map_err(|_| "AMQP channel already initialized".to_string())?;

    Ok(endpoint)
}

/// Publish a block.indexed event. Silent no-op if AMQP not initialized.
///
/// `network` is the Bitcoin network name ("mainnet" | "testnet" | "signet" |
/// "regtest") — included in the payload so consumers don't have to parse the
/// routing key, and can cleanly branch on mainnet vs testnet events coming
/// through the same exchange.
pub async fn publish_block_event(
    routing_key: &str,
    network: &str,
    block_height: u64,
    block_hash: &str,
    reveals: u64,
    transfers: u64,
    elapsed_ms: u64,
) -> Result<(), String> {
    let channel = match CHANNEL.get() {
        Some(ch) => ch,
        None => return Ok(()), // not configured
    };
    let exchange = match EXCHANGE_NAME.get() {
        Some(ex) => ex.as_str(),
        None => return Ok(()),
    };

    let payload = serde_json::json!({
        "event": "block.indexed",
        "network": network,
        "block_height": block_height,
        "block_hash": block_hash,
        "reveals": reveals,
        "transfers": transfers,
        "elapsed_ms": elapsed_ms,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    let args = BasicPublishArguments::new(exchange, routing_key);

    channel
        .basic_publish(
            BasicProperties::default()
                .with_content_type("application/json")
                .with_delivery_mode(2) // persistent
                .finish(),
            payload.to_string().into_bytes(),
            args,
        )
        .await
        .map_err(|e| format!("AMQP publish failed: {e}"))?;

    Ok(())
}
