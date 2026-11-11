//! Helpers shared by the integration tests.

#![allow(dead_code)]

use std::sync::OnceLock;

use secp256k1::Keypair;
use secp256k1::schnorr;
use serde_json::json;
use sha2::{Digest, Sha256};

/// Every event in the sample file that verifies, each with its index.
/// Verified once per test binary: it is most of a debug test's time.
pub fn sample() -> &'static [(Vec<u8>, Vec<u8>)] {
    static SAMPLE: OnceLock<Vec<(Vec<u8>, Vec<u8>)>> = OnceLock::new();
    SAMPLE.get_or_init(|| {
        std::fs::read("tests/data/many-events-sample.jsonl")
            .unwrap()
            .split(|&c| c == b'\n')
            .filter(|l| !l.is_empty())
            .filter_map(|l| Some((l.to_vec(), noq::build_note(l).ok()?)))
            .collect()
    })
}

pub fn events() -> Vec<Vec<u8>> {
    sample().iter().map(|(json, _)| json.clone()).collect()
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Signs fresh events, for cases the sample file doesn't have.
pub struct Signer {
    keypair: Keypair,
    pub pubkey: [u8; 32],
}

impl Signer {
    /// A key derived from `seed`; the same seed is the same key.
    pub fn new(seed: u8) -> Signer {
        let keypair = Keypair::from_secret_bytes([seed.max(1); 32]).unwrap();
        let pubkey = keypair.x_only_public_key().0.to_byte_array();
        Signer { keypair, pubkey }
    }

    /// A signed event's json. Content and tag values must not hold control
    /// characters: serde_json escapes those differently from NIP-01's
    /// canonical form.
    pub fn event(&self, created_at: u64, kind: u32, tags: &[&[&str]], content: &str) -> Vec<u8> {
        let pubkey = hex(&self.pubkey);
        let canon = json!([0, pubkey, created_at, kind, tags, content]).to_string();
        let id: [u8; 32] = Sha256::digest(canon.as_bytes()).into();
        let sig = schnorr::sign_no_aux_rand(&id, &self.keypair);
        json!({
            "id": hex(&id),
            "pubkey": pubkey,
            "created_at": created_at,
            "kind": kind,
            "tags": tags,
            "content": content,
            "sig": hex(&sig.to_byte_array()),
        })
        .to_string()
        .into_bytes()
    }
}
