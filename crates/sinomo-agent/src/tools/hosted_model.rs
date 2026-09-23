use super::{
    AgentTool, AgentToolArtifactRef, AgentToolCapability, AgentToolDescriptor, AgentToolEvidence,
    AgentToolInvocationMode, AgentToolRegistry, AgentToolRequest, AgentToolResult,
};
use crate::{AgentToolKind, AgentToolSpec};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use sinomo_lm::{
    LanguageModelProviderCapabilities, OPENAI_MAX_IMAGE_REFERENCES, OpenAiImageRequest,
    OpenAiImageResponse, OpenAiProvider, OpenAiResponsesCodexClient, OpenAiWebSearchContextSize,
    OpenAiWebSearchRequest, OpenAiWebSearchResponse,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MAX_REFERENCE_IMAGES: usize = OPENAI_MAX_IMAGE_REFERENCES;
const MAX_REFERENCE_IMAGE_BYTES: u64 = 25 * 1024 * 1024;

const IMAGE_GENERATION_DESCRIPTION: &str = "\
Generate an image from a description, or edit existing images with specific instructions. Use it when:

- The user asks for an image of a described scene, such as a diagram, portrait, comic, meme, or any other visual.
- The user wants an existing image modified, including adding or removing elements, altering colors, \
improving quality, or transforming the style (for example cartoon or oil painting).

Guidelines:
- Image generation takes a while to finish; treat a call as long-running rather than retrying it.
- Omit `referenced_image_paths` when generating a brand new image.
- For edits, pass every target image through `referenced_image_paths`, up to five paths that resolve \
inside the granted workspace roots.
- If you have not seen a referenced image yet, inspect it before editing so the prompt describes the \
intended change.
- If a target image has no path inside the workspace, ask the user to save or attach it before editing.
- Generate the image directly without asking for confirmation unless a required image must be supplied again.
- Prefer this tool over shell or scripting tools for image editing unless the user explicitly asks otherwise.";

pub trait HostedModelToolBackend: Send + Sync {
    fn capabilities(&self) -> LanguageModelProviderCapabilities;

    fn web_search(
        &self,
        request: &OpenAiWebSearchRequest,
    ) -> Result<OpenAiWebSearchResponse, String>;

    fn generate_or_edit_image(
        &self,
        request: &OpenAiImageRequest,
    ) -> Result<OpenAiImageResponse, String>;
}

impl HostedModelToolBackend for OpenAiProvider {
    fn capabilities(&self) -> LanguageModelProviderCapabilities {
        LanguageModelProviderCapabilities::openai()
    }

    fn web_search(
        &self,
        request: &OpenAiWebSearchRequest,
    ) -> Result<OpenAiWebSearchResponse, String> {
        OpenAiProvider::web_search(self, request).map_err(|error| error.to_string())
    }

    fn generate_or_edit_image(
        &self,
        request: &OpenAiImageRequest,
    ) -> Result<OpenAiImageResponse, String> {
        OpenAiProvider::generate_or_edit_image(self, request).map_err(|error| error.to_string())
    }
}

/// A ChatGPT sign-in reaches the same image model and a standalone web search endpoint through the
/// Codex backend, so both hosted tools resolve against it.
impl HostedModelToolBackend for OpenAiResponsesCodexClient {
    fn capabilities(&self) -> LanguageModelProviderCapabilities {
        LanguageModelProviderCapabilities::codex()
    }

    fn web_search(
        &self,
        request: &OpenAiWebSearchRequest,
    ) -> Result<OpenAiWebSearchResponse, String> {
        OpenAiResponsesCodexClient::web_search(self, request).map_err(|error| error.to_string())
    }

    fn generate_or_edit_image(
        &self,
        request: &OpenAiImageRequest,
    ) -> Result<OpenAiImageResponse, String> {
        OpenAiResponsesCodexClient::generate_or_edit_image(self, request)
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone)]
pub struct HostedModelAgentToolConfig {
    backend: Arc<dyn HostedModelToolBackend>,
    artifact_root: PathBuf,
    allowed_image_roots: Vec<PathBuf>,
}

impl HostedModelAgentToolConfig {
    pub fn new(
        backend: Arc<dyn HostedModelToolBackend>,
        artifact_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            backend,
            artifact_root: artifact_root.into(),
            allowed_image_roots: Vec::new(),
        }
    }

    pub fn with_allowed_image_roots(
        mut self,
        roots: impl IntoIterator<Item = impl Into<PathBuf>>,
    ) -> Self {
        self.allowed_image_roots = roots.into_iter().map(Into::into).collect();
        self
    }
}

pub fn register_hosted_model_tools(
    registry: &AgentToolRegistry,
    config: HostedModelAgentToolConfig,
) {
    let capabilities = config.backend.capabilities();
    if capabilities.web_search {
        registry.register(WebSearchTool {
            backend: config.backend.clone(),
        });
    }
    if capabilities.image_generation {
        registry.register(ImageGenerationTool { config });
    }
}

#[derive(Clone)]
struct WebSearchTool {
    backend: Arc<dyn HostedModelToolBackend>,
}

impl AgentTool for WebSearchTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        web_search_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if request.cancellation_token.is_cancelled() {
            return AgentToolResult::cancelled(
                "web search was cancelled before execution",
                serde_json::json!({ "error": "cancelled" }),
            );
        }
        let Some(query) = input_string(&request.call.input, "query") else {
            return AgentToolResult::failed(
                "web_search requires a non-empty query",
                serde_json::json!({ "error": "missing_query" }),
            );
        };
        let allowed_domains = input_string_array(&request.call.input, "allowed_domains");
        let context_size = match input_string(&request.call.input, "search_context_size").as_deref()
        {
            Some("low") => OpenAiWebSearchContextSize::Low,
            Some("high") => OpenAiWebSearchContextSize::High,
            _ => OpenAiWebSearchContextSize::Medium,
        };
        let result = match self.backend.web_search(&OpenAiWebSearchRequest {
            query: query.clone(),
            model: None,
            allowed_domains,
            context_size,
        }) {
            Ok(result) => result,
            Err(detail) => {
                return AgentToolResult::failed(
                    format!("web search failed: {detail}"),
                    serde_json::json!({ "error": "web_search_failed", "detail": detail }),
                );
            }
        };
        let output = serde_json::json!({
            "query": query,
            "model": result.model,
            "text": result.text,
            "sources": result.sources,
        });
        AgentToolResult::succeeded(
            format!(
                "web search returned {} source{}",
                result.sources.len(),
                if result.sources.len() == 1 { "" } else { "s" }
            ),
            output.clone(),
        )
        .with_evidence(
            AgentToolEvidence::availability_only(
                true,
                "hosted search executed through a configured model provider",
            )
            .with_output_value(&output, false)
            .with_metadata(serde_json::json!({
                "provider_capability": "web_search",
                "source_count": result.sources.len(),
            })),
        )
    }
}

#[derive(Clone)]
struct ImageGenerationTool {
    config: HostedModelAgentToolConfig,
}

impl AgentTool for ImageGenerationTool {
    fn descriptor(&self) -> AgentToolDescriptor {
        image_generation_descriptor()
    }

    fn execute(&self, request: AgentToolRequest<'_>) -> AgentToolResult {
        if request.cancellation_token.is_cancelled() {
            return AgentToolResult::cancelled(
                "image generation was cancelled before execution",
                serde_json::json!({ "error": "cancelled" }),
            );
        }
        let Some(prompt) = input_string(&request.call.input, "prompt") else {
            return AgentToolResult::failed(
                "image_generation requires a non-empty prompt",
                serde_json::json!({ "error": "missing_prompt" }),
            );
        };
        let reference_paths = input_string_array(&request.call.input, "referenced_image_paths");
        if reference_paths.len() > MAX_REFERENCE_IMAGES {
            return AgentToolResult::failed(
                format!("image_generation accepts at most {MAX_REFERENCE_IMAGES} reference images"),
                serde_json::json!({ "error": "too_many_reference_images" }),
            );
        }
        let referenced_images =
            match load_reference_images(&reference_paths, &self.config.allowed_image_roots) {
                Ok(images) => images,
                Err(detail) => {
                    return AgentToolResult::blocked(
                        detail.clone(),
                        serde_json::json!({ "error": "image_path_not_allowed", "detail": detail }),
                    );
                }
            };
        let result = match self
            .config
            .backend
            .generate_or_edit_image(&OpenAiImageRequest {
                prompt: prompt.clone(),
                referenced_images,
                size: input_string(&request.call.input, "size"),
                quality: input_string(&request.call.input, "quality"),
                background: input_string(&request.call.input, "background"),
            }) {
            Ok(result) => result,
            Err(detail) => {
                return AgentToolResult::failed(
                    format!("image generation failed: {detail}"),
                    serde_json::json!({ "error": "image_generation_failed", "detail": detail }),
                );
            }
        };
        let extension = normalized_image_extension(&result.output_format);
        let file_name = format!(
            "{}-{}.{}",
            sanitize_path_component(request.session.session_id.as_str()),
            sanitize_path_component(request.call_id.as_str()),
            extension,
        );
        if let Err(source) = fs::create_dir_all(&self.config.artifact_root) {
            return AgentToolResult::failed(
                format!("failed to create generated-image artifact directory: {source}"),
                serde_json::json!({ "error": "artifact_directory_failed", "detail": source.to_string() }),
            );
        }
        let path = self.config.artifact_root.join(file_name);
        if let Err(source) = fs::write(&path, &result.bytes) {
            return AgentToolResult::failed(
                format!("failed to save generated image: {source}"),
                serde_json::json!({ "error": "artifact_write_failed", "detail": source.to_string() }),
            );
        }
        let output = serde_json::json!({
            "model": result.model,
            "path": path,
            "media_type": result.media_type,
            "bytes": result.bytes.len(),
            "size": result.size,
            "operation": if reference_paths.is_empty() { "generation" } else { "edit" },
            "output_hint": image_generation_output_hint(&path),
        });
        AgentToolResult::succeeded(
            format!("saved generated image to {}", path.display()),
            output.clone(),
        )
        .with_evidence(
            AgentToolEvidence::availability_only(
                true,
                "image request executed through a configured provider and references stayed inside workspace grants",
            )
            .with_output_value(&output, false)
            .with_artifact(
                AgentToolArtifactRef::new("generated_image", path.display().to_string())
                    .with_label("Generated image")
                    .with_media_type(result.media_type),
            )
            .with_metadata(serde_json::json!({
                "provider_capability": "image_generation",
                "reference_image_count": reference_paths.len(),
            })),
        )
    }
}

pub fn web_search_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::WebSearch,
        "web_search",
        "Search the public web for current information and return source URLs with the answer.",
        false,
    ))
    .with_input_schema(serde_json::json!({
        "type": "object",
        "required": ["query"],
        "properties": {
            "query": { "type": "string" },
            "allowed_domains": {
                "type": "array",
                "items": { "type": "string" },
                "maxItems": 20
            },
            "search_context_size": {
                "type": "string",
                "enum": ["low", "medium", "high"]
            }
        },
        "additionalProperties": false
    }))
    .with_output_schema(serde_json::json!({
        "type": "object",
        "required": ["query", "model", "text", "sources"],
        "properties": {
            "query": { "type": "string" },
            "model": { "type": "string" },
            "text": { "type": "string" },
            "sources": { "type": "array" }
        }
    }))
    .with_capabilities([
        AgentToolCapability::AccessNetwork,
        AgentToolCapability::CallModel,
    ])
    .with_invocation_modes(AgentToolInvocationMode::all())
    .supporting_parallel(true)
}

pub fn image_generation_descriptor() -> AgentToolDescriptor {
    AgentToolDescriptor::new(AgentToolSpec::new(
        AgentToolKind::ImageGeneration,
        "image_generation",
        IMAGE_GENERATION_DESCRIPTION,
        false,
    ))
    .with_input_schema(serde_json::json!({
        "type": "object",
        "required": ["prompt"],
        "properties": {
            "prompt": { "type": "string" },
            "referenced_image_paths": {
                "type": "array",
                "items": { "type": "string" },
                "maxItems": MAX_REFERENCE_IMAGES
            },
            "size": { "type": "string", "enum": ["auto", "1024x1024", "1024x1536", "1536x1024"] },
            "quality": { "type": "string", "enum": ["auto", "low", "medium", "high"] },
            "background": { "type": "string", "enum": ["auto", "opaque", "transparent"] }
        },
        "additionalProperties": false
    }))
    .with_output_schema(serde_json::json!({
        "type": "object",
        "required": ["model", "path", "media_type", "bytes", "operation"]
    }))
    .with_capabilities([
        AgentToolCapability::AccessNetwork,
        AgentToolCapability::CallModel,
        AgentToolCapability::ReadWorkspace,
    ])
    .with_invocation_modes(AgentToolInvocationMode::all())
}

/// Tool details expose the artifact; Markdown opts into a preview in the assistant response.
fn image_generation_output_hint(path: &Path) -> String {
    let path = path.display().to_string().replace('\\', "/");
    format!(
        "The generated image is saved at {path} and can be inspected in the tool details. \
To show it in your response, include ![Generated image](<{path}>). A regular Markdown file link \
can also reference the result. Reference the existing file; there is no need to save or render it \
again. Leave the original in place unless the user asks for it to be removed."
    )
}

fn load_reference_images(
    paths: &[String],
    allowed_roots: &[PathBuf],
) -> Result<Vec<String>, String> {
    paths
        .iter()
        .map(|path| load_reference_image(path, allowed_roots))
        .collect()
}

fn load_reference_image(path: &str, allowed_roots: &[PathBuf]) -> Result<String, String> {
    if allowed_roots.is_empty() {
        return Err("image editing requires at least one granted workspace root".to_string());
    }
    let requested = Path::new(path);
    let candidates = if requested.is_absolute() {
        vec![requested.to_path_buf()]
    } else {
        allowed_roots
            .iter()
            .map(|root| root.join(requested))
            .collect()
    };
    let resolved = candidates
        .into_iter()
        .find_map(|candidate| candidate.canonicalize().ok())
        .ok_or_else(|| format!("reference image `{path}` does not exist"))?;
    let allowed = allowed_roots.iter().any(|root| {
        root.canonicalize()
            .ok()
            .is_some_and(|root| resolved.starts_with(root))
    });
    if !allowed {
        return Err(format!(
            "reference image `{}` is outside the granted workspace roots",
            resolved.display()
        ));
    }
    let metadata = fs::metadata(&resolved)
        .map_err(|source| format!("inspect reference image `{}`: {source}", resolved.display()))?;
    if !metadata.is_file() {
        return Err(format!(
            "reference image `{}` is not a file",
            resolved.display()
        ));
    }
    if metadata.len() > MAX_REFERENCE_IMAGE_BYTES {
        return Err(format!(
            "reference image `{}` exceeds the {} MiB limit",
            resolved.display(),
            MAX_REFERENCE_IMAGE_BYTES / (1024 * 1024),
        ));
    }
    let media_type = image_media_type(&resolved)?;
    let bytes = fs::read(&resolved)
        .map_err(|source| format!("read reference image `{}`: {source}", resolved.display()))?;
    Ok(format!(
        "data:{media_type};base64,{}",
        BASE64_STANDARD.encode(bytes)
    ))
}

fn image_media_type(path: &Path) -> Result<&'static str, String> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "png" => Ok("image/png"),
        "jpg" | "jpeg" => Ok("image/jpeg"),
        "webp" => Ok("image/webp"),
        _ => Err(format!(
            "reference image `{}` must be PNG, JPEG, or WebP",
            path.display()
        )),
    }
}

fn input_string(value: &serde_json::Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn input_string_array(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn sanitize_path_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => character,
            _ => '-',
        })
        .collect::<String>();
    let sanitized = sanitized.trim_matches('-');
    if sanitized.is_empty() {
        "image".to_string()
    } else {
        sanitized.to_string()
    }
}

fn normalized_image_extension(value: &str) -> &'static str {
    match value.trim().to_ascii_lowercase().as_str() {
        "jpeg" | "jpg" => "jpg",
        "webp" => "webp",
        _ => "png",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_generation_hint_uses_markdown_to_reference_the_existing_file() {
        let hint = image_generation_output_hint(Path::new("C:\\images\\my picture.png"));
        assert!(hint.contains("![Generated image](<C:/images/my picture.png>)"));
        assert!(hint.contains("Reference the existing file"));
    }

    /// A backend that registers tools without reaching a provider, so registration is tested
    /// purely against the declared capabilities.
    struct CapabilityOnlyBackend(LanguageModelProviderCapabilities);

    impl HostedModelToolBackend for CapabilityOnlyBackend {
        fn capabilities(&self) -> LanguageModelProviderCapabilities {
            self.0
        }

        fn web_search(
            &self,
            _request: &OpenAiWebSearchRequest,
        ) -> Result<OpenAiWebSearchResponse, String> {
            unreachable!("registration must not call the backend")
        }

        fn generate_or_edit_image(
            &self,
            _request: &OpenAiImageRequest,
        ) -> Result<OpenAiImageResponse, String> {
            unreachable!("registration must not call the backend")
        }
    }

    fn registered_hosted_tools(
        capabilities: LanguageModelProviderCapabilities,
    ) -> AgentToolRegistry {
        let registry = AgentToolRegistry::new();
        register_hosted_model_tools(
            &registry,
            HostedModelAgentToolConfig::new(
                Arc::new(CapabilityOnlyBackend(capabilities)),
                PathBuf::from("/tmp/sinomo-hosted-tools-test"),
            ),
        );
        registry
    }

    #[test]
    fn hosted_tools_register_for_every_provider_that_declares_the_capability() {
        // An OpenAI API key and a Codex ChatGPT sign-in both reach gpt-image-2 and a hosted search,
        // so both credentials expose the same pair of tools through different backends.
        for capabilities in [
            LanguageModelProviderCapabilities::openai(),
            LanguageModelProviderCapabilities::codex(),
        ] {
            let registry = registered_hosted_tools(capabilities);
            assert!(registry.contains("image_generation"));
            assert!(registry.contains("web_search"));
        }
    }

    #[test]
    fn image_generation_stays_unregistered_without_a_supporting_credential() {
        let unsupported =
            registered_hosted_tools(LanguageModelProviderCapabilities::local_responses());
        assert!(!unsupported.contains("image_generation"));
        assert!(!unsupported.contains("web_search"));
    }

    #[test]
    fn hosted_descriptors_are_shared_across_turn_based_and_realtime_modes() {
        for descriptor in [web_search_descriptor(), image_generation_descriptor()] {
            assert!(descriptor.supports_invocation_mode(AgentToolInvocationMode::TurnBased));
            assert!(descriptor.supports_invocation_mode(AgentToolInvocationMode::Realtime));
            assert!(
                descriptor
                    .capabilities
                    .contains(&AgentToolCapability::AccessNetwork)
            );
        }
    }

    #[test]
    fn reference_images_are_confined_to_workspace_roots() {
        let root = sinomo_testdir::TestDir::new("hosted-model-image-root");
        let inside = root.path().join("input.png");
        fs::write(&inside, b"png").expect("write input");
        let image = load_reference_image("input.png", &[root.path().to_path_buf()])
            .expect("load allowed input");
        assert!(image.starts_with("data:image/png;base64,"));

        let outside = sinomo_testdir::TestDir::new("hosted-model-image-outside");
        let outside_path = outside.path().join("outside.png");
        fs::write(&outside_path, b"png").expect("write outside input");
        let error = load_reference_image(
            outside_path.to_str().expect("utf8 path"),
            &[root.path().to_path_buf()],
        )
        .expect_err("outside path must be blocked");
        assert!(error.contains("outside the granted workspace roots"));
    }
}
