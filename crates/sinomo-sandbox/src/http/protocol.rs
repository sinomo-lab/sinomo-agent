use crate::internal::*;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum SandboxHttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Head,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxHttpRequest {
    pub method: SandboxHttpMethod,
    pub url: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body: Vec<u8>,
    pub timeout_ms: u64,
    pub max_response_bytes: u64,
}

impl SandboxHttpRequest {
    pub fn get(url: impl Into<String>) -> Self {
        Self {
            method: SandboxHttpMethod::Get,
            url: url.into(),
            headers: BTreeMap::new(),
            body: Vec::new(),
            timeout_ms: default_sandbox_http_timeout_ms(),
            max_response_bytes: default_sandbox_http_response_bytes(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxHttpResponse {
    pub status: u16,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxHttpExchangeRecord {
    pub request_id: SandboxNetworkRequestId,
    pub sandbox_id: SandboxId,
    pub command_id: SandboxCommandId,
    pub method: SandboxHttpMethod,
    pub url: String,
    pub host: String,
    pub status: Option<u16>,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub redacted_headers: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxHttpUrlParts {
    pub scheme: String,
    pub host: String,
    pub redacted_url: String,
}

impl SandboxHttpUrlParts {
    pub(crate) fn parse(url: &str) -> Result<Self, SandboxPolicyDenial> {
        let (scheme, rest) = url.split_once("://").ok_or_else(|| {
            SandboxPolicyDenial::new("network-url-invalid", format!("invalid URL `{url}`"))
        })?;
        let authority = rest
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default()
            .trim();
        if scheme.is_empty() || authority.is_empty() {
            return Err(SandboxPolicyDenial::new(
                "network-url-invalid",
                format!("invalid URL `{url}`"),
            ));
        }
        let authority_without_userinfo = authority
            .rsplit_once('@')
            .map_or(authority, |(_userinfo, authority)| authority);
        let (host, redacted_authority) =
            if let Some(rest) = authority_without_userinfo.strip_prefix('[') {
                let (host, rest) = rest.split_once(']').ok_or_else(|| {
                    SandboxPolicyDenial::new("network-url-invalid", format!("invalid URL `{url}`"))
                })?;
                let host = host.to_ascii_lowercase();
                let port = if rest.is_empty() {
                    None
                } else if let Some(port) = rest.strip_prefix(':') {
                    if port.is_empty() || !port.chars().all(|ch| ch.is_ascii_digit()) {
                        return Err(SandboxPolicyDenial::new(
                            "network-url-invalid",
                            format!("invalid URL `{url}`"),
                        ));
                    }
                    Some(port)
                } else {
                    return Err(SandboxPolicyDenial::new(
                        "network-url-invalid",
                        format!("invalid URL `{url}`"),
                    ));
                };
                let redacted_authority = if let Some(port) = port {
                    format!("[{host}]:{port}")
                } else {
                    format!("[{host}]")
                };
                (host, redacted_authority)
            } else {
                if authority_without_userinfo.matches(':').count() > 1 {
                    return Err(SandboxPolicyDenial::new(
                        "network-url-invalid",
                        format!("invalid URL `{url}`"),
                    ));
                }
                let (host, port) =
                    if let Some((host, port)) = authority_without_userinfo.rsplit_once(':') {
                        if port.is_empty() || !port.chars().all(|ch| ch.is_ascii_digit()) {
                            return Err(SandboxPolicyDenial::new(
                                "network-url-invalid",
                                format!("invalid URL `{url}`"),
                            ));
                        }
                        (host.to_ascii_lowercase(), Some(port))
                    } else {
                        (authority_without_userinfo.to_ascii_lowercase(), None)
                    };
                let redacted_authority = if let Some(port) = port {
                    format!("{host}:{port}")
                } else {
                    host.clone()
                };
                (host, redacted_authority)
            };
        if host.is_empty() {
            return Err(SandboxPolicyDenial::new(
                "network-url-invalid",
                format!("invalid URL `{url}`"),
            ));
        }
        let path = rest
            .strip_prefix(authority)
            .unwrap_or_default()
            .split(['?', '#'])
            .next()
            .unwrap_or_default();
        Ok(Self {
            scheme: scheme.to_ascii_lowercase(),
            redacted_url: format!(
                "{}://{}{}",
                scheme.to_ascii_lowercase(),
                redacted_authority,
                path
            ),
            host,
        })
    }
}

#[derive(Debug, Error)]
pub enum SandboxNetworkError {
    #[error("sandbox network policy denied: {0}")]
    PolicyDenied(SandboxPolicyDenial),
    #[error("sandbox network transport error: {detail}")]
    Transport { detail: String },
    #[error("sandbox network response is {actual} bytes; max is {limit}")]
    ResponseTooLarge { actual: u64, limit: u64 },
}

#[derive(Debug, Error)]
pub enum SandboxRecordedHttpFetchError {
    #[error(transparent)]
    Network(#[from] SandboxNetworkError),
    #[error(transparent)]
    Store(#[from] SandboxStoreError),
    #[error(transparent)]
    Fs(#[from] SandboxFsError),
    #[error("sandbox network request could not be audited: {0:?}")]
    AuditPolicy(SandboxPolicyDenial),
}

pub trait SandboxHttpTransport {
    fn fetch(
        &self,
        request: &SandboxHttpRequest,
    ) -> Result<SandboxHttpResponse, SandboxNetworkError>;
}

pub fn sandbox_http_fetch<T: SandboxHttpTransport>(
    policy: &SandboxPolicy,
    transport: &T,
    request: &SandboxHttpRequest,
) -> Result<SandboxHttpResponse, SandboxNetworkError> {
    policy
        .validate_http_request(request)
        .map_err(SandboxNetworkError::PolicyDenied)?;
    let response = transport.fetch(request)?;
    if response.body.len() as u64 > request.max_response_bytes {
        return Err(SandboxNetworkError::ResponseTooLarge {
            actual: response.body.len() as u64,
            limit: request.max_response_bytes,
        });
    }
    Ok(response)
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRecordedHttpResponse {
    pub request_id: SandboxNetworkRequestId,
    pub response: SandboxHttpResponse,
    pub record: SandboxHttpExchangeRecord,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCreateInstanceRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_id: Option<SandboxId>,
    pub toolchain_id: SandboxToolchainId,
    pub temp_policy: SandboxTempPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_workspace_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_written_bytes_per_command: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_file_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<SandboxMount>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCreateInstanceResponse {
    pub manifest: SandboxInstanceManifest,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxStatusRequest {
    pub sandbox_id: SandboxId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxStatusResponse {
    pub manifest: SandboxInstanceManifest,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxUpdateInstanceRequest {
    pub sandbox_id: SandboxId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<SandboxPath>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env_set: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env_unset: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxUpdateInstanceResponse {
    pub manifest: SandboxInstanceManifest,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCheckpointRequest {
    pub sandbox_id: SandboxId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxCheckpointResponse {
    pub checkpoint: SandboxCheckpoint,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxResumeRequest {
    pub sandbox_id: SandboxId,
    pub checkpoint_id: SandboxCheckpointId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxResumeResponse {
    pub manifest: SandboxInstanceManifest,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRebootRequest {
    pub sandbox_id: SandboxId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRebootResponse {
    pub manifest: SandboxInstanceManifest,
    pub cleared_tmp: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxDestroyRequest {
    pub sandbox_id: SandboxId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxDestroyResponse {
    pub sandbox_id: SandboxId,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxListRequest {
    pub sandbox_id: SandboxId,
    pub path: SandboxPath,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxListResponse {
    pub entries: Vec<SandboxDirEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxReadRequest {
    pub sandbox_id: SandboxId,
    pub path: SandboxPath,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub range: Option<ByteRange>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxReadResponse {
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWriteRequest {
    pub sandbox_id: SandboxId,
    pub path: SandboxPath,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxWriteResponse {
    pub path: SandboxPath,
    pub bytes_written: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxImportBusyBoxToolchainRequest {
    pub version: String,
    pub provenance: String,
    pub module_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxImportBusyBoxToolchainResponse {
    pub manifest: SandboxToolchainManifest,
    pub module_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxImportToolchainPackageRequest {
    pub toolchain_id: SandboxToolchainId,
    pub version: String,
    #[serde(default = "default_sandbox_wasi_abi")]
    pub target_abi: SandboxWasiAbi,
    pub provenance: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<SandboxImportToolchainPackageCommand>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<SandboxImportToolchainPackageArtifact>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxImportToolchainPackageFromPathRequest {
    pub package_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolchain_id: Option<SandboxToolchainId>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxImportToolchainPackageCommand {
    pub name: String,
    pub module_bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_abi: Option<SandboxWasiAbi>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub argv_prefix: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub default_env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub default_limits: SandboxResourceLimits,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxImportToolchainPackageArtifact {
    pub relative_path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxImportToolchainPackageResponse {
    pub manifest: SandboxToolchainManifest,
    pub package_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxAddToolchainCommandRequest {
    pub toolchain_id: SandboxToolchainId,
    pub version: String,
    #[serde(default = "default_sandbox_wasi_abi")]
    pub target_abi: SandboxWasiAbi,
    pub provenance: String,
    pub command_name: String,
    pub module_bytes: Vec<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_capabilities: Vec<String>,
    pub default_limits: SandboxResourceLimits,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxAddToolchainCommandResponse {
    pub manifest: SandboxToolchainManifest,
    pub module_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxAddToolchainArtifactRequest {
    pub toolchain_id: SandboxToolchainId,
    pub version: String,
    pub provenance: String,
    pub relative_path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxAddToolchainArtifactResponse {
    pub manifest: SandboxToolchainManifest,
    pub artifact_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRunToolchainRequest {
    pub sandbox_id: SandboxId,
    pub command_name: String,
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdin: Option<Vec<u8>>,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRunProcessPlanRequest {
    pub sandbox_id: SandboxId,
    pub plan: SandboxProcessPlan,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxRunCancelResponse {
    pub operation_id: String,
    pub cancelled: bool,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SandboxRunToolchainStreamFrame {
    Output { chunk: SandboxOutputChunk },
    Complete { outcome: SandboxLocalCommandOutcome },
    Error { detail: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum SandboxRunProcessPlanStreamFrame {
    Output { chunk: SandboxOutputChunk },
    Complete { outcome: SandboxProcessPlanOutcome },
    Error { detail: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxHttpFetchToFileRequest {
    pub sandbox_id: SandboxId,
    pub request: SandboxHttpRequest,
    pub output_path: SandboxPath,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct SandboxHttpFetchFileResult {
    pub request_id: SandboxNetworkRequestId,
    pub output_path: SandboxPath,
    pub status: u16,
    pub response_bytes: u64,
    pub record: SandboxHttpExchangeRecord,
}

pub fn sandbox_http_fetch_recording<T, F>(
    policy: &SandboxPolicy,
    transport: &T,
    sandbox_fs: &mut F,
    sandbox_id: SandboxId,
    command_id: SandboxCommandId,
    request: &SandboxHttpRequest,
) -> Result<SandboxRecordedHttpResponse, SandboxRecordedHttpFetchError>
where
    T: SandboxHttpTransport,
    F: SandboxFileSystem,
{
    policy
        .validate_http_request(request)
        .map_err(SandboxNetworkError::PolicyDenied)?;
    let request_id = SandboxNetworkRequestId::generate();
    let response = transport.fetch(request);
    let record = sandbox_http_exchange_record_with_id(
        request_id.clone(),
        sandbox_id,
        command_id,
        request,
        response.as_ref().ok(),
    )
    .map_err(SandboxRecordedHttpFetchError::AuditPolicy)?;
    persist_http_exchange_record(sandbox_fs, &record)?;
    let response = response?;
    if response.body.len() as u64 > request.max_response_bytes {
        return Err(SandboxNetworkError::ResponseTooLarge {
            actual: response.body.len() as u64,
            limit: request.max_response_bytes,
        }
        .into());
    }
    Ok(SandboxRecordedHttpResponse {
        request_id,
        response,
        record,
    })
}

pub fn sandbox_http_fetch_to_file<T, F>(
    policy: &SandboxPolicy,
    transport: &T,
    sandbox_fs: &mut F,
    sandbox_id: SandboxId,
    command_id: SandboxCommandId,
    request: &SandboxHttpRequest,
    output_path: &SandboxPath,
    limits: &SandboxResourceLimits,
) -> Result<SandboxHttpFetchFileResult, SandboxRecordedHttpFetchError>
where
    T: SandboxHttpTransport,
    F: SandboxFileSystem,
{
    let recorded = sandbox_http_fetch_recording(
        policy, transport, sandbox_fs, sandbox_id, command_id, request,
    )?;
    validate_sandbox_write_resource_limits(
        sandbox_fs,
        output_path,
        recorded.response.body.len() as u64,
        limits,
    )?;
    sandbox_fs.write(output_path, &recorded.response.body)?;
    Ok(SandboxHttpFetchFileResult {
        request_id: recorded.request_id,
        output_path: output_path.clone(),
        status: recorded.response.status,
        response_bytes: recorded.response.body.len() as u64,
        record: recorded.record,
    })
}

pub fn sandbox_http_exchange_record(
    sandbox_id: SandboxId,
    command_id: SandboxCommandId,
    request: &SandboxHttpRequest,
    response: Option<&SandboxHttpResponse>,
) -> Result<SandboxHttpExchangeRecord, SandboxPolicyDenial> {
    sandbox_http_exchange_record_with_id(
        SandboxNetworkRequestId::generate(),
        sandbox_id,
        command_id,
        request,
        response,
    )
}

pub fn sandbox_http_exchange_record_with_id(
    request_id: SandboxNetworkRequestId,
    sandbox_id: SandboxId,
    command_id: SandboxCommandId,
    request: &SandboxHttpRequest,
    response: Option<&SandboxHttpResponse>,
) -> Result<SandboxHttpExchangeRecord, SandboxPolicyDenial> {
    let parts = SandboxHttpUrlParts::parse(&request.url)?;
    Ok(SandboxHttpExchangeRecord {
        request_id,
        sandbox_id,
        command_id,
        method: request.method.clone(),
        url: parts.redacted_url,
        host: parts.host,
        status: response.map(|response| response.status),
        request_bytes: request.body.len() as u64,
        response_bytes: response.map_or(0, |response| response.body.len() as u64),
        redacted_headers: request
            .headers
            .keys()
            .filter(|key| sandbox_http_header_is_sensitive(key))
            .cloned()
            .collect(),
    })
}

fn sandbox_http_header_is_sensitive(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie"
    )
}
