//! Retired source-chain authentication is confined to the converter.
use super::{MetadataBackendError, Result};
use sinomo_dfs::DfsMetadataPrefixProof;
fn error(message: impl std::fmt::Display) -> MetadataBackendError {
    MetadataBackendError::Serde(message.to_string())
}
fn digest(version: u16, parts: &[&[u8]]) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(if version == 1 {
        b"sinomo-dfs-metadata-prefix-proof\0v1\0"
    } else {
        b"sinomo-dfs-metadata-prefix-proof\0v2\0"
    });
    for part in parts {
        hash.update(&(part.len() as u64).to_be_bytes());
        hash.update(part);
    }
    format!(
        "dfs-metadata-prefix-v{version}:blake3:{}",
        hash.finalize().to_hex()
    )
}
pub(super) fn validate(proof: &DfsMetadataPrefixProof) -> Result<()> {
    if !matches!(proof.version, 1 | 2)
        || proof
            .digest
            .strip_prefix(&format!("dfs-metadata-prefix-v{}:blake3:", proof.version))
            .is_none_or(|hash| {
                hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
    {
        return Err(error("invalid retained source prefix proof"));
    }
    Ok(())
}
pub(super) fn extend_as(
    previous: &DfsMetadataPrefixProof,
    sequence: u64,
    exact_frame: &[u8],
    version: u16,
) -> Result<DfsMetadataPrefixProof> {
    validate(previous)?;
    if !matches!(version, 1 | 2) || previous.version > version {
        return Err(error("invalid retained prefix version transition"));
    }
    if previous.journal_sequence.checked_add(1) != Some(sequence) {
        return Err(error("retained source journal prefix has a sequence gap"));
    }
    let sequence_bytes = sequence.to_be_bytes();
    let value = if version == 1 {
        let json = super::wire_json::journal_json(exact_frame)?;
        digest(
            1,
            &[
                b"journal-entry",
                previous.digest.as_bytes(),
                &sequence_bytes,
                &json,
            ],
        )
    } else {
        digest(
            2,
            &[
                b"journal-frame",
                &previous.version.to_be_bytes(),
                &previous.journal_sequence.to_be_bytes(),
                previous.digest.as_bytes(),
                &sequence_bytes,
                exact_frame,
            ],
        )
    };
    Ok(DfsMetadataPrefixProof {
        version,
        journal_sequence: sequence,
        digest: value,
    })
}
/// A conversion base represents this exact retained source and current
/// projection. It is not an empty genesis; the shard's activation certifies
/// its image.
pub(super) fn conversion_base(
    sequence: u64,
    source_cut: &str,
    converted_projection: &str,
) -> DfsMetadataPrefixProof {
    DfsMetadataPrefixProof {
        version: 2,
        journal_sequence: sequence,
        digest: digest(
            2,
            &[
                b"storage-import-v7",
                source_cut.as_bytes(),
                converted_projection.as_bytes(),
                &sequence.to_be_bytes(),
            ],
        ),
    }
}
