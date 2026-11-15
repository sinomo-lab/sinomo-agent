//! Bounded byte continuation of an existing parent payload receipt. This owns
//! no source traversal cursor, inventory, scheduler or lifetime.
use super::*;
use sinomo_dfs::{
    ContentImportBlobPublication, ContentImportBlobVerifier, ContentStoreStagingWriter,
};
use std::sync::atomic::AtomicBool;
use std::time::Instant;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct CopyCursor {
    pub source: BlobSource,
    pub copied: u64,
    pub staged: PathBuf,
    pub existing: Option<PathBuf>,
}
pub(super) struct WarmCopy {
    pub key: PathBuf,
    pub verifier: ContentImportBlobVerifier,
}

pub(super) fn copy_step(
    root: &Path,
    target: &Path,
    source: &BlobSource,
    receipt: &Path,
    cursor: &mut Option<CopyCursor>,
    warm: &mut Option<WarmCopy>,
    writer: &ContentStoreStagingWriter,
    bytes_left: &mut u64,
    deadline: Instant,
) -> Result<bool> {
    let hex = super::incremental::hash_hex(&source.hash)?;
    if source.path.is_absolute()
        || source
            .path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(error("legacy copy escaped its retained source"));
    }
    let input = root.join(&source.path);
    let canonical = fs::canonicalize(&input).map_err(|e| io(&input, e))?;
    if !canonical.starts_with(root) {
        return Err(error("legacy copy resolves outside its original root"));
    }
    let staging = target.join("legacy-copy-staging");
    if cursor.is_none() {
        durable_file::ensure_directory(&staging).map_err(error)?;
        let id = blake3::hash(receipt.to_string_lossy().as_bytes())
            .to_hex()
            .to_string();
        *cursor = Some(CopyCursor {
            source: source.clone(),
            copied: 0,
            staged: staging.join(id),
            existing: None,
        });
    }
    let state = cursor.as_mut().unwrap();
    if state.source.hash != source.hash
        || state.source.path != source.path
        || state.source.offset != source.offset
        || state.source.size != source.size
        || state.copied > source.size
        || state.staged.parent() != Some(staging.as_path())
    {
        return Err(error(
            "legacy payload receipt changed its original byte identity",
        ));
    }
    let destination = target.join("blobs").join(&hex[..2]).join(hex);
    if state
        .existing
        .as_ref()
        .is_some_and(|path| path != &destination)
    {
        return Err(error("legacy existing-byte proof changed target"));
    }
    if state.copied > 0 && !state.staged.exists() && state.existing.is_none() {
        if destination.is_file() {
            state.existing = Some(destination.clone());
        } else {
            state.copied = 0;
            *warm = None;
        }
    }
    if state.copied < source.size && state.existing.is_none() {
        if *bytes_left == 0 || Instant::now() >= deadline {
            return Ok(false);
        }
        let mut input_file = File::open(&canonical).map_err(|e| io(&canonical, e))?;
        let source_length = input_file.metadata().map_err(|e| io(&canonical, e))?.len();
        if source
            .offset
            .checked_add(source.size)
            .is_none_or(|end| end > source_length)
        {
            return Err(error("legacy blob span exceeds its retained source"));
        }
        input_file
            .seek(SeekFrom::Start(source.offset + state.copied))
            .map_err(|e| io(&canonical, e))?;
        let mut output = if state.staged.exists() {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&state.staged)
                .map_err(|e| io(&state.staged, e))?
        } else {
            if state.copied != 0 {
                return Err(error("legacy staged byte prefix disappeared"));
            }
            durable_file::create_private_staged_file(&state.staged).map_err(error)?
        };
        output
            .set_len(state.copied)
            .map_err(|e| io(&state.staged, e))?;
        output
            .seek(SeekFrom::Start(state.copied))
            .map_err(|e| io(&state.staged, e))?;
        let mut copied = state.copied;
        let mut buffer = [0; 64 * 1024];
        while copied < source.size && *bytes_left != 0 && Instant::now() < deadline {
            let count = (source.size - copied)
                .min(*bytes_left)
                .min(buffer.len() as u64) as usize;
            input_file
                .read_exact(&mut buffer[..count])
                .map_err(|e| io(&canonical, e))?;
            output
                .write_all(&buffer[..count])
                .map_err(|e| io(&state.staged, e))?;
            copied += count as u64;
            *bytes_left -= count as u64;
        }
        output.sync_all().map_err(|e| io(&state.staged, e))?;
        state.copied = copied;
        if copied < source.size {
            return Ok(false);
        }
    }
    if warm.as_ref().is_none_or(|held| held.key != receipt) {
        let verifier = if let Some(path) = &state.existing {
            ContentImportBlobVerifier::existing(path, source.hash.clone(), source.size)
                .map_err(error)?
        } else {
            if !state.staged.exists() {
                let file =
                    durable_file::create_private_staged_file(&state.staged).map_err(error)?;
                drop(file);
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&state.staged)
                .map_err(|e| io(&state.staged, e))?;
            let synced =
                durable_file::sync_private_staged_file(&state.staged, file).map_err(error)?;
            ContentImportBlobVerifier::new(synced, source.hash.clone(), source.size)
                .map_err(error)?
        };
        *warm = Some(WarmCopy {
            key: receipt.to_owned(),
            verifier,
        });
    }
    if *bytes_left == 0 || Instant::now() >= deadline {
        return Ok(false);
    }
    let held = warm.as_mut().unwrap();
    let before = held.verifier.verified_bytes();
    let done = held
        .verifier
        .step(*bytes_left, deadline, &AtomicBool::new(false))
        .map_err(error)?;
    *bytes_left -= held.verifier.verified_bytes() - before;
    if !done || Instant::now() >= deadline {
        return Ok(false);
    }
    let held = warm.take().unwrap();
    match writer
        .publish_verified_candidate_blob(held.verifier.finish().map_err(error)?)
        .map_err(error)?
    {
        ContentImportBlobPublication::VerifyCurrent { path } => {
            state.existing = Some(path);
            Ok(false)
        }
        ContentImportBlobPublication::CurrentDeleted => Err(error(
            "legacy source would resurrect a retired candidate blob",
        )),
        ContentImportBlobPublication::Published => {
            if state.staged.exists() {
                durable_file::remove_file_if_exists_and_sync_parent(&state.staged)
                    .map_err(error)?;
            }
            *cursor = None;
            Ok(true)
        }
    }
}
