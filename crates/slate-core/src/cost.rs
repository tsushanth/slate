/// Egress cost estimates per GB (USD) for cross-region/cross-provider transfers.
/// Same-region, same-provider transfers are free.
/// Source: AWS/GCP/Azure published pricing as of 2026.

pub struct EgressCost {
    pub bytes: u64,
    pub provider: &'static str,
    pub rate_per_gb: f64,
    pub estimated_usd: f64,
}

pub fn estimate(src_url: &str, bytes: u64) -> EgressCost {
    let (provider, rate_per_gb) = if src_url.starts_with("s3://") {
        ("AWS S3", 0.09)
    } else if src_url.starts_with("gs://") {
        ("Google Cloud Storage", 0.12)
    } else if src_url.starts_with("az://") {
        ("Azure Blob Storage", 0.087)
    } else {
        ("local", 0.0)
    };

    let gb = bytes as f64 / 1_073_741_824.0; // 1024^3
    EgressCost {
        bytes,
        provider,
        rate_per_gb,
        estimated_usd: gb * rate_per_gb,
    }
}
