//! Configure a fresh app through its normal daemon API without displaying secrets.
//! Usage: configure_mobile_test_provider LOOPBACK_ADDRESS CREDENTIAL_FILE MODEL
//! The credential file is an existing node-local DeepSeek credential document.
//! Existing accounts are rejected; a failed mutation is not retried by this tool.

use sinomo_app::backend::config::{add_provider_account, load_provider_config};
use sinomo_runtime::api::RemoteConfigClient;
use std::{net::SocketAddr, time::Duration};

fn main() -> Result<(), String> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() != 4 {
        return Err(
            "usage: configure_mobile_test_provider LOOPBACK_ADDRESS CREDENTIAL_FILE MODEL".into(),
        );
    }
    let address = args[1]
        .parse::<SocketAddr>()
        .map_err(|_| "invalid address")?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err("use the app's explicit local/ADB-forwarded control endpoint".into());
    }
    let credential: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&args[2]).map_err(|_| "cannot read credential file")?,
    )
    .map_err(|_| "invalid credential document")?;
    let key = credential
        .pointer("/value/api_key")
        .and_then(|key| key.as_str())
        .filter(|key| !key.is_empty())
        .ok_or("credential has no API key")?;
    let client = RemoteConfigClient::new(address.to_string(), Duration::from_secs(15));
    let before = load_provider_config(&client).ok_or("cannot read provider configuration")?;
    if !before.accounts.is_empty() {
        return Err("provider accounts already exist; refusing to replace them".into());
    }
    add_provider_account(&client, "deepseek", "default", key, &args[3], "")
        .map_err(|_| "provider configuration failed; outcome must be checked before retry")?;
    println!(
        "configured DeepSeek/default with model {} through daemon RPC",
        args[3]
    );
    Ok(())
}
