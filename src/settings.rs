use bitcoin::Network;
use std::sync::Arc;

/// Immutable chain context derived from the connected Bitcoin node.
pub struct RuntimeSettings {
    network: String,
}

impl RuntimeSettings {
    pub fn new(
        pool_cfg: &crate::config::PoolConfig,
        node_chain: &str,
    ) -> anyhow::Result<Arc<Self>> {
        let network = match node_chain {
            "main" => "mainnet",
            "test" => "testnet",
            "testnet4" => "testnet4",
            "signet" => "signet",
            "regtest" => "regtest",
            other => anyhow::bail!("node reports unrecognized chain \"{other}\""),
        };

        if let Some(asserted) = &pool_cfg.network {
            if asserted != network {
                anyhow::bail!(
                    "[pool] network = \"{asserted}\" but the connected node is on {network} - \
                     refusing to start. Point the pool at a {asserted} node, or fix/remove the \
                     network assertion in config.toml"
                );
            }
        }

        Ok(Arc::new(Self {
            network: network.to_string(),
        }))
    }

    pub fn network(&self) -> &str {
        &self.network
    }

    pub fn bitcoin_network(&self) -> Network {
        match self.network.as_str() {
            "mainnet" => Network::Bitcoin,
            "testnet" => Network::Testnet,
            "testnet4" => Network::Testnet4,
            "signet" => Network::Signet,
            "regtest" => Network::Regtest,
            _ => unreachable!("RuntimeSettings only stores recognized node networks"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PoolConfig;

    fn pool_cfg(network_assert: Option<&str>) -> PoolConfig {
        PoolConfig {
            listen_addr: "127.0.0.1:0".into(),
            coinbase_address: None,
            coinbase_tag: "/test/".into(),
            initial_difficulty: 1,
            extranonce1_size: 4,
            extranonce2_size: 4,
            max_connections: 8,
            idle_timeout_secs: 300,
            found_block_dir: "found-blocks".into(),
            network: network_assert.map(str::to_string),
        }
    }

    #[test]
    fn network_is_derived_from_node_chain() {
        let s = RuntimeSettings::new(&pool_cfg(None), "test").unwrap();
        assert_eq!(s.network(), "testnet");
        assert_eq!(s.bitcoin_network(), Network::Testnet);
        assert!(RuntimeSettings::new(&pool_cfg(None), "weirdchain").is_err());
    }

    #[test]
    fn configured_network_assertion_is_enforced() {
        assert!(RuntimeSettings::new(&pool_cfg(Some("mainnet")), "main").is_ok());
        assert!(RuntimeSettings::new(&pool_cfg(Some("testnet")), "main").is_err());
    }
}
