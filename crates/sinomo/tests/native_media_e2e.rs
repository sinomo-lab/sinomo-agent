use serde_json::Value;
use sinomo_testdir::TestDir;
use std::env;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

#[test]
#[ignore = "requires virtual native mic/camera/speaker devices; see tests/media/setup_virtual_media.sh"]
fn native_media_voice_call_reaches_fake_realtime_endpoint() {
    if env::var("SINOMO_TEST_NATIVE_MEDIA").ok().as_deref() != Some("1") {
        eprintln!("skipping: set SINOMO_TEST_NATIVE_MEDIA=1 after creating virtual media devices");
        return;
    }
    let mic_device = required_env("SINOMO_TEST_MIC_DEVICE");
    let camera_device = required_env("SINOMO_TEST_CAMERA_DEVICE");
    let speaker_device = required_env("SINOMO_TEST_SPEAKER_DEVICE");
    let temp = TestDir::new("native-media-e2e");
    let summary_path = temp.join("fake-realtime-summary.json");
    let mut server = FakeRealtimeServer::start(&summary_path);
    let output_path = temp.join("voice-output.raw");
    let sinomo = env!("CARGO_BIN_EXE_sinomo");

    let output = Command::new(sinomo)
        .env("SINOMO_DATA_DIR", temp.path())
        .env("SINOMO_MINICPM_API_KEY", "fake-test-key")
        .arg("voice")
        .arg("call")
        .arg("--provider")
        .arg("minicpm-o")
        .arg("--model")
        .arg("MiniCPM-o-4.5")
        .arg("--endpoint")
        .arg(&server.websocket_url)
        .arg("--native-mic")
        .arg("--mic-device")
        .arg(&mic_device)
        .arg("--native-camera")
        .arg("--camera-device")
        .arg(&camera_device)
        .arg("--native-speaker")
        .arg("--speaker-device")
        .arg(&speaker_device)
        .arg("--call-ms")
        .arg("600")
        .arg("--chunk-ms")
        .arg("100")
        .arg("--video-frame-ms")
        .arg("250")
        .arg("--receive-timeout-ms")
        .arg("5000")
        .arg("--output")
        .arg(&output_path)
        .output()
        .expect("run sinomo voice call");

    server.wait_for_exit(Duration::from_secs(5));
    let summary = read_summary(&summary_path);
    assert!(
        output.status.success(),
        "sinomo voice call failed\nstdout:\n{}\nstderr:\n{}\nsummary:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        summary
    );
    assert!(
        summary
            .get("audio_chunks")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            > 0,
        "fake realtime server did not receive native audio: {summary}"
    );
    assert!(
        summary
            .get("video_frames")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            > 0,
        "fake realtime server did not receive native video: {summary}"
    );
    let output_len = fs::metadata(&output_path)
        .map(|metadata| metadata.len())
        .unwrap_or_default();
    assert!(output_len > 0, "expected output audio sidecar bytes");
}

struct FakeRealtimeServer {
    child: Child,
    websocket_url: String,
}

impl FakeRealtimeServer {
    fn start(summary_path: &Path) -> Self {
        let script = repo_root().join("tests/media/fake_realtime_server.py");
        let python = env::var("PYTHON").unwrap_or_else(|_| "python3".to_string());
        let mut child = Command::new(python)
            .arg(&script)
            .arg("--summary")
            .arg(summary_path)
            .arg("--response-delay-ms")
            .arg("1000")
            .arg("--min-audio-chunks")
            .arg("1")
            .arg("--min-video-frames")
            .arg("1")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|error| {
                panic!("spawn fake realtime server {}: {error}", script.display())
            });
        let stdout = child.stdout.take().expect("fake server stdout");
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        reader
            .read_line(&mut line)
            .expect("read fake server ready line");
        let ready: Value = serde_json::from_str(&line).unwrap_or_else(|error| {
            panic!("parse fake server ready JSON `{}`: {error}", line.trim())
        });
        let websocket_url = ready
            .get("websocket_url")
            .and_then(Value::as_str)
            .expect("fake server websocket_url")
            .to_string();
        Self {
            child,
            websocket_url,
        }
    }

    fn wait_for_exit(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for FakeRealtimeServer {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn required_env(name: &str) -> String {
    env::var(name).unwrap_or_else(|_| panic!("{name} must be set when SINOMO_TEST_NATIVE_MEDIA=1"))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("repo root")
        .to_path_buf()
}

fn read_summary(path: &Path) -> Value {
    let text = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read fake realtime summary {}: {error}", path.display()));
    serde_json::from_str(&text)
        .unwrap_or_else(|error| panic!("parse fake realtime summary {}: {error}", path.display()))
}
