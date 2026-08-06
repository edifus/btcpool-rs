use crate::error::PoolError;
use bitcoin::{address::NetworkUnchecked, Address, Network, ScriptBuf};

/// A network-checked Bitcoin payout destination extracted from a miner identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayoutDescriptor {
    pub address: String,
    pub script_pubkey: ScriptBuf,
}

/// The identity authorized by a miner.
///
/// The full identity remains the worker key used by statistics, while the part
/// before the first dot is the Bitcoin address placed in that miner's jobs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MinerIdentity {
    pub full_name: String,
    pub payout: PayoutDescriptor,
    pub worker_label: Option<String>,
}

impl MinerIdentity {
    pub fn parse(raw: &str, network: Network, max_len: usize) -> Result<Self, PoolError> {
        validate_identity_text(raw, max_len)?;

        let (address, worker_label) = match raw.split_once('.') {
            Some((_address, "")) => {
                return Err(invalid_identity("worker label after '.' must not be empty"));
            }
            Some((address, label)) => (address, Some(label.to_string())),
            None => (raw, None),
        };

        if address.is_empty() {
            return Err(invalid_identity("payout address must not be empty"));
        }

        let parsed: Address<NetworkUnchecked> = address
            .parse()
            .map_err(|_| invalid_identity("payout address is not a valid Bitcoin address"))?;
        let checked = parsed.require_network(network).map_err(|_| {
            invalid_identity(&format!(
                "payout address is not valid for {}",
                network_name(network)
            ))
        })?;

        Ok(Self {
            full_name: raw.to_string(),
            payout: PayoutDescriptor {
                address: checked.to_string(),
                script_pubkey: checked.script_pubkey(),
            },
            worker_label,
        })
    }
}

/// Reuse the one implementation of the untrusted-name rules, in
/// [`crate::security::validate_worker_name`]. Both protocol frontends run that
/// check before calling [`MinerIdentity::parse`]; repeating the rules here — as
/// this used to — meant two places to keep in step, with the identity copy
/// silently governing anything that reached `parse` by another route.
fn validate_identity_text(raw: &str, max_len: usize) -> Result<(), PoolError> {
    crate::security::validate_worker_name(raw, max_len).map_err(|e| match e {
        PoolError::InvalidParams { detail, .. } => invalid_identity(&detail),
        other => other,
    })
}

fn invalid_identity(detail: &str) -> PoolError {
    PoolError::InvalidParams {
        method: "miner identity",
        detail: detail.to_string(),
    }
}

fn network_name(network: Network) -> &'static str {
    match network {
        Network::Bitcoin => "mainnet",
        Network::Testnet => "testnet",
        Network::Testnet4 => "testnet4",
        Network::Signet => "signet",
        Network::Regtest => "regtest",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn addr_for(network: Network) -> String {
        let pk = bitcoin::CompressedPublicKey::from_str(
            "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
        )
        .unwrap();
        Address::p2wpkh(&pk, network).to_string()
    }

    #[test]
    fn parses_address_with_optional_worker_label() {
        let address = addr_for(Network::Bitcoin);
        let bare = MinerIdentity::parse(&address, Network::Bitcoin, 128).unwrap();
        assert_eq!(bare.full_name, address);
        assert_eq!(bare.worker_label, None);

        let raw = format!("{address}.bitaxe.01");
        let worker = MinerIdentity::parse(&raw, Network::Bitcoin, 128).unwrap();
        assert_eq!(worker.full_name, raw);
        assert_eq!(worker.payout.address, address);
        assert_eq!(worker.worker_label.as_deref(), Some("bitaxe.01"));
    }

    #[test]
    fn payout_script_is_derived_from_checked_address() {
        let address = addr_for(Network::Bitcoin);
        let identity = MinerIdentity::parse(&address, Network::Bitcoin, 128).unwrap();
        let checked = address
            .parse::<Address<NetworkUnchecked>>()
            .unwrap()
            .require_network(Network::Bitcoin)
            .unwrap();
        assert_eq!(identity.payout.script_pubkey, checked.script_pubkey());
    }

    #[test]
    fn rejects_wrong_network_and_bad_addresses() {
        let testnet = addr_for(Network::Testnet);
        assert!(MinerIdentity::parse(&testnet, Network::Bitcoin, 128).is_err());
        assert!(MinerIdentity::parse("not-an-address.worker", Network::Bitcoin, 128).is_err());
    }

    #[test]
    fn testnet_addresses_are_valid_on_signet_and_testnet4() {
        let address = addr_for(Network::Testnet);
        assert!(MinerIdentity::parse(&address, Network::Signet, 128).is_ok());
        assert!(MinerIdentity::parse(&address, Network::Testnet4, 128).is_ok());
    }

    #[test]
    fn rejects_empty_suffix_whitespace_and_overlong_identity() {
        let address = addr_for(Network::Bitcoin);
        assert!(MinerIdentity::parse(&format!("{address}."), Network::Bitcoin, 128).is_err());
        assert!(
            MinerIdentity::parse(&format!("{address}.bad worker"), Network::Bitcoin, 128).is_err()
        );
        assert!(MinerIdentity::parse(
            &format!("{address}.{}", "x".repeat(128)),
            Network::Bitcoin,
            128
        )
        .is_err());
    }
}
