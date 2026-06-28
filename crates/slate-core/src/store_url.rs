use anyhow::{bail, Result};
use object_store::{
    aws::AmazonS3Builder,
    azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem,
    memory::InMemory,
    path::Path,
    ClientOptions, ObjectStore,
};
use std::sync::Arc;
use url::Url;

pub struct ResolvedStore {
    pub store: Arc<dyn ObjectStore>,
    pub prefix: Path,
}

/// Client options tuned for high-throughput parallel chunk transfers:
/// - HTTP/2 multiplexes all chunk requests over one TCP connection (eliminates per-chunk handshake)
/// - Large connection pool so concurrent objects don't stall waiting for a slot
/// - Keep-alive pings prevent the connection from going idle between chunks
fn cloud_client_options() -> ClientOptions {
    use std::time::Duration;
    // allow_http2: negotiate HTTP/2 where the server supports it (S3 supports it for data
    // transfers but not all control-plane ops, so we don't force http2_only)
    ClientOptions::new()
        .with_allow_http2()
        .with_pool_max_idle_per_host(32)
        .with_http2_keep_alive_interval(Duration::from_secs(5))
        .with_http2_keep_alive_timeout(Duration::from_secs(15))
        .with_http2_keep_alive_while_idle()
        .with_connect_timeout(Duration::from_secs(10))
}

pub fn resolve(raw: &str) -> Result<ResolvedStore> {
    if raw.starts_with("s3://") {
        let url = Url::parse(raw)?;
        let bucket = url.host_str().unwrap_or("").to_string();
        let key = url.path().trim_start_matches('/').to_string();
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(&bucket)
            .with_client_options(cloud_client_options())
            .build()?;
        Ok(ResolvedStore {
            store: Arc::new(store),
            prefix: Path::from(key.as_str()),
        })
    } else if raw.starts_with("gs://") {
        let url = Url::parse(raw)?;
        let bucket = url.host_str().unwrap_or("").to_string();
        let key = url.path().trim_start_matches('/').to_string();
        let store = GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(&bucket)
            .with_client_options(cloud_client_options())
            .build()?;
        Ok(ResolvedStore {
            store: Arc::new(store),
            prefix: Path::from(key.as_str()),
        })
    } else if raw.starts_with("az://") {
        let url = Url::parse(raw)?;
        let container = url.host_str().unwrap_or("").to_string();
        let key = url.path().trim_start_matches('/').to_string();
        let store = MicrosoftAzureBuilder::from_env()
            .with_container_name(&container)
            .with_client_options(cloud_client_options())
            .build()?;
        Ok(ResolvedStore {
            store: Arc::new(store),
            prefix: Path::from(key.as_str()),
        })
    } else if raw.starts_with("file://") || raw.starts_with('/') {
        let path = raw.strip_prefix("file://").unwrap_or(raw);
        let store = LocalFileSystem::new_with_prefix(path)?;
        Ok(ResolvedStore {
            store: Arc::new(store),
            prefix: Path::from(""),
        })
    } else if raw == "mem://" {
        Ok(ResolvedStore {
            store: Arc::new(InMemory::new()),
            prefix: Path::from(""),
        })
    } else {
        bail!("Unsupported store URL: {raw}. Use s3://, gs://, az://, file://, or /path")
    }
}
