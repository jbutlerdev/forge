//! Embedding + reranking for the semantic message router.
//!
//! Uses the lab's Qwen3-Embedding-4B (2560-dim) and Qwen3-Reranker-4B
//! models, served via Bifrost (`embeddings/qwen3-embedding-4b` and
//! `embeddings/qwen3-reranker-4b`). See `../lab/ct/embeddings/README.md`.
//!
//! ## Config
//!
//! - `FORGE_EMBEDDING_URL` (default `http://bitfrost.botnet:8080`) —
//!   the base URL for `/v1/embeddings`.
//! - `FORGE_EMBEDDING_MODEL` (default `embeddings/qwen3-embedding-4b`).
//! - `FORGE_EMBEDDING_API_KEY` (default `bifrost`).
//! - `FORGE_RERANKER_URL` (default `http://bitfrost.botnet:8080`) —
//!   the base URL for the reranker's `/v1/chat/completions`.
//! - `FORGE_RERANKER_MODEL` (default `embeddings/qwen3-reranker-4b`).
//! - `FORGE_RERANKER_API_KEY` (default `bifrost`).
//!
//! If the embedding endpoint is unreachable, the semantic router
//! degrades gracefully: it falls back to the LLM-classification path
//! (the old approach), so a missing embeddings backend never breaks
//! routing.

use serde::Deserialize;

/// The embedding dimension for Qwen3-Embedding-4B. Used to validate
/// responses and to sanity-check stored vectors.
pub const EMBEDDING_DIM: usize = 2560;

/// Configuration resolved from env vars at startup. Stored in
/// `AppState` so we read the env once, not per-request.
#[derive(Debug, Clone)]
pub struct EmbeddingConfig {
    pub embedding_url: String,
    pub embedding_model: String,
    pub embedding_api_key: String,
    pub reranker_url: String,
    pub reranker_model: String,
    pub reranker_api_key: String,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            embedding_url: std::env::var("FORGE_EMBEDDING_URL")
                .unwrap_or_else(|_| "http://bitfrost.botnet:8080".to_string()),
            embedding_model: std::env::var("FORGE_EMBEDDING_MODEL")
                .unwrap_or_else(|_| "embeddings/qwen3-embedding-4b".to_string()),
            embedding_api_key: std::env::var("FORGE_EMBEDDING_API_KEY")
                .unwrap_or_else(|_| "bifrost".to_string()),
            reranker_url: std::env::var("FORGE_RERANKER_URL")
                .unwrap_or_else(|_| "http://bitfrost.botnet:8080".to_string()),
            reranker_model: std::env::var("FORGE_RERANKER_MODEL")
                .unwrap_or_else(|_| "embeddings/qwen3-reranker-4b".to_string()),
            reranker_api_key: std::env::var("FORGE_RERANKER_API_KEY")
                .unwrap_or_else(|_| "bifrost".to_string()),
        }
    }
}

/// Response from `/v1/embeddings`.
#[derive(Debug, Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

/// Embed a single text string via the Qwen3-Embedding-4B model.
/// Returns the 2560-dim vector, or an error if the endpoint is
/// unreachable or returns a malformed response.
pub async fn embed(config: &EmbeddingConfig, text: &str) -> Result<Vec<f32>, EmbedError> {
    let url = format!(
        "{}/v1/embeddings",
        config.embedding_url.trim_end_matches('/')
    );
    let body = serde_json::json!({
        "model": config.embedding_model,
        "input": text,
    });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| EmbedError::Client(e.to_string()))?;

    let mut req = client.post(&url).json(&body);
    if !config.embedding_api_key.is_empty() {
        req = req.bearer_auth(&config.embedding_api_key);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| EmbedError::Request(e.to_string()))?;
    let status = resp.status();
    let text_resp = resp
        .text()
        .await
        .map_err(|e| EmbedError::Read(e.to_string()))?;
    if !status.is_success() {
        return Err(EmbedError::Status(
            status.as_u16(),
            text_resp.chars().take(300).collect(),
        ));
    }

    let parsed: EmbeddingsResponse =
        serde_json::from_str(&text_resp).map_err(|e| EmbedError::Parse(e.to_string()))?;

    let embedding = parsed
        .data
        .into_iter()
        .next()
        .ok_or(EmbedError::NoData)?
        .embedding;

    if embedding.len() != EMBEDDING_DIM {
        return Err(EmbedError::DimMismatch(embedding.len()));
    }

    Ok(embedding)
}

/// Rerank a (query, document) pair using the Qwen3-Reranker-4B model.
/// The reranker is prompt-based (not a Jina-style `/v1/rerank`
/// endpoint): we build a yes/no prompt and parse the response.
/// Returns `true` if the document is relevant to the query.
pub async fn rerank(
    config: &EmbeddingConfig,
    query: &str,
    document: &str,
) -> Result<bool, EmbedError> {
    let url = format!(
        "{}/v1/chat/completions",
        config.reranker_url.trim_end_matches('/')
    );
    // Truncate the document to keep the prompt small (the reranker
    // has a 4k context window). 500 chars is plenty for a session
    // summary.
    let doc_trunc = if document.len() > 500 {
        &document[..500]
    } else {
        document
    };
    let prompt = format!(
        "Query: {}\nDocument: {}\nIs this document relevant to the query? Answer yes or no.",
        query, doc_trunc
    );

    let body = serde_json::json!({
        "model": config.reranker_model,
        "messages": [
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "max_tokens": 128,
    });

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| EmbedError::Client(e.to_string()))?;

    let mut req = client.post(&url).json(&body);
    if !config.reranker_api_key.is_empty() {
        req = req.bearer_auth(&config.reranker_api_key);
    }

    let resp = req
        .send()
        .await
        .map_err(|e| EmbedError::Request(e.to_string()))?;
    let status = resp.status();
    let text_resp = resp
        .text()
        .await
        .map_err(|e| EmbedError::Read(e.to_string()))?;
    if !status.is_success() {
        return Err(EmbedError::Status(
            status.as_u16(),
            text_resp.chars().take(300).collect(),
        ));
    }

    let v: serde_json::Value =
        serde_json::from_str(&text_resp).map_err(|e| EmbedError::Parse(e.to_string()))?;

    // The reranker may put the answer in `content` or, if it's a
    // reasoning model, in `reasoning`. Try content first, then
    // reasoning as a fallback.
    let content = v
        .get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("");

    let answer = if !content.is_empty() {
        content
    } else {
        v.get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("reasoning"))
            .and_then(|r| r.as_str())
            .unwrap_or("")
    };

    // Parse yes/no. Tolerant: "yes", "Yes", "YES", "yes.", "yes\n"...
    let lower = answer.to_lowercase();
    let is_yes = lower.starts_with("yes");
    let is_no = lower.starts_with("no");

    // If neither, default to false (don't route to a session the
    // reranker was unsure about).
    if !is_yes && !is_no {
        tracing::warn!(
            "reranker returned neither yes nor no: {:?}",
            &answer[..answer.len().min(100)]
        );
    }

    Ok(is_yes)
}

/// Cosine similarity between two vectors. Returns 0.0 for empty or
/// mismatched-length vectors (shouldn't happen with validated
/// embeddings, but defensive).
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

/// Errors from the embedding/reranking endpoints. All carry enough
/// context to log; none expose secrets.
#[derive(Debug)]
pub enum EmbedError {
    Client(String),
    Request(String),
    Read(String),
    Status(u16, String),
    Parse(String),
    NoData,
    DimMismatch(usize),
}

impl std::fmt::Display for EmbedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Client(s) => write!(f, "HTTP client build failed: {}", s),
            Self::Request(s) => write!(f, "Request failed: {}", s),
            Self::Read(s) => write!(f, "Read body failed: {}", s),
            Self::Status(code, body) => write!(f, "HTTP {}: {}", code, body),
            Self::Parse(s) => write!(f, "Parse failed: {}", s),
            Self::NoData => write!(f, "No embedding in response"),
            Self::DimMismatch(d) => write!(f, "Expected {} dims, got {}", EMBEDDING_DIM, d),
        }
    }
}

impl std::error::Error for EmbedError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_identical_vectors() {
        let v = vec![1.0, 2.0, 3.0, 4.0];
        let sim = cosine_similarity(&v, &v);
        assert!((sim - 1.0).abs() < 1e-5, "identical vectors should be 1.0");
    }

    #[test]
    fn cosine_orthogonal_vectors() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let sim = cosine_similarity(&a, &b);
        assert!(sim.abs() < 1e-5, "orthogonal vectors should be 0.0");
    }

    #[test]
    fn cosine_opposite_vectors() {
        let a = vec![1.0, 0.0];
        let b = vec![-1.0, 0.0];
        let sim = cosine_similarity(&a, &b);
        assert!(
            (sim - (-1.0)).abs() < 1e-5,
            "opposite vectors should be -1.0"
        );
    }

    #[test]
    fn cosine_mismatched_lengths() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![1.0, 2.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }

    #[test]
    fn cosine_empty_vectors() {
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
    }

    #[test]
    fn cosine_zero_vector() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        assert_eq!(cosine_similarity(&a, &b), 0.0);
    }

    #[test]
    fn cosine_high_dim_2560() {
        // Simulate 2560-dim vectors (the real embedding size) to
        // make sure the loop is fast enough for in-app use.
        let a: Vec<f32> = (0..EMBEDDING_DIM).map(|i| (i as f32) * 0.001).collect();
        let b: Vec<f32> = (0..EMBEDDING_DIM).map(|i| (i as f32) * 0.001).collect();
        let start = std::time::Instant::now();
        let sim = cosine_similarity(&a, &b);
        let elapsed = start.elapsed();
        assert!((sim - 1.0).abs() < 1e-3);
        // Should be well under 1ms for 2560 dims.
        assert!(
            elapsed.as_millis() < 10,
            "cosine over {} dims took {:?}",
            EMBEDDING_DIM,
            elapsed
        );
    }

    #[test]
    fn config_defaults_to_bifrost() {
        // The defaults should point at bifrost with the lab's model
        // ids. (This test doesn't set env vars, so it gets the
        // defaults — but env vars could override it in CI. Just
        // check the structure is sane.)
        let cfg = EmbeddingConfig::default();
        assert!(!cfg.embedding_url.is_empty());
        assert!(!cfg.embedding_model.is_empty());
        assert!(!cfg.reranker_url.is_empty());
        assert!(!cfg.reranker_model.is_empty());
    }
}
