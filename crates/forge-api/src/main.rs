//! `forge-api` binary. All startup logic lives in the library crate
//! (`forge_api::run`); this is a thin wrapper.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    forge_api::run().await
}
