package net.sinomo.app.use;

import android.accessibilityservice.AccessibilityService;
import android.accessibilityservice.GestureDescription;
import android.graphics.Path;
import android.net.LocalServerSocket;
import android.net.LocalSocket;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.Process;
import android.view.accessibility.AccessibilityEvent;
import android.view.accessibility.AccessibilityNodeInfo;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/** An OS adapter reachable only by this application's UID. Never opens a TCP port. */
public final class AppUseAccessibilityService extends AccessibilityService {
    private final Handler main = new Handler(Looper.getMainLooper());
    private final Set<LocalSocket> connections = ConcurrentHashMap.newKeySet();
    private final Semaphore capacity = new Semaphore(24);
    private LocalServerSocket server;
    private volatile boolean stopped = true;
    @Override public void onAccessibilityEvent(AccessibilityEvent event) {}
    @Override public void onInterrupt() { closeBridge(); }
    @Override public void onDestroy() { closeBridge(); super.onDestroy(); }
    @Override protected void onServiceConnected() {
        if (!stopped) return;
        try {
            server = new LocalServerSocket("sinomo-app-use:" + Process.myUid());
            stopped = false;
            new Thread(() -> accept(), "app-use-local").start();
        } catch (IOException error) {
            android.util.Log.e("SinomoAppUse", "Unable to open local OS bridge", error);
            closeBridge();
        }
    }
    private void closeBridge() {
        stopped = true;
        try { if (server != null) server.close(); } catch (IOException ignored) {}
        for (LocalSocket socket : connections) try { socket.close(); } catch (IOException ignored) {}
        connections.clear();
        synchronized (AppUseProjectionService.LOCK) {
            if (AppUseProjectionService.current != null) AppUseProjectionService.current.stopSelf();
        }
    }
    private void accept() {
        while (!stopped) {
            try {
                LocalSocket socket = server.accept();
                int peer = socket.getPeerCredentials().getUid();
                if (peer != Process.myUid() || !capacity.tryAcquire()) {
                    android.util.Log.w("SinomoAppUse", "Rejected local OS bridge peer or capacity: uid=" + peer);
                    socket.close(); continue;
                }
                socket.setSoTimeout(5000);
                connections.add(socket);
                new Thread(() -> serve(socket), "app-use-client").start();
            } catch (IOException error) {
                if (!stopped) {
                    android.util.Log.e("SinomoAppUse", "Local OS bridge listener failed", error);
                    closeBridge();
                }
            }
        }
    }
    private static final class Failure extends Exception {
        final String code;
        Failure(String code, String message) { super(message); this.code = code; }
    }
    private static Failure unavailable() { return new Failure("target_unavailable", "screen share ended or display geometry changed; obtain local consent and enumerate again"); }
    private void validate(JSONObject target) throws Exception {
        if (stopped) throw new Failure("permission_denied", "accessibility service is disabled");
        synchronized (AppUseProjectionService.LOCK) {
            if (!"monitor".equals(target.optString("kind"))) throw new Failure("unsupported", "Android requires explicit raw mode");
            if (AppUseProjectionService.target == null || !AppUseProjectionService.target.equals(target.optString("device_name"))) throw unavailable();
            android.util.DisplayMetrics metrics = new android.util.DisplayMetrics();
            android.view.Display display = getSystemService(android.view.WindowManager.class).getDefaultDisplay();
            display.getRealMetrics(metrics);
            if (metrics.widthPixels != AppUseProjectionService.width || metrics.heightPixels != AppUseProjectionService.height
                    || display.getRotation() != AppUseProjectionService.rotation) {
                AppUseProjectionService.target = null;
                if (AppUseProjectionService.current != null) AppUseProjectionService.current.stopSelf();
                throw unavailable();
            }
        }
    }
    private static void respond(DataOutputStream output, JSONObject value) throws IOException {
        byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 65536) throw new IOException("oversized response");
        output.writeInt(bytes.length); output.write(bytes); output.flush();
    }
    private void serve(LocalSocket socket) {
        boolean capture = false;
        AppUseGpuCapture.Client captureClient = null;
        AppUseProjectionService captureOwner = null;
        Input input = null;
        try (LocalSocket connection = socket;
             DataInputStream in = new DataInputStream(connection.getInputStream());
             DataOutputStream out = new DataOutputStream(connection.getOutputStream())) {
            while (!stopped) {
                int length = in.readInt();
                if (length <= 0 || length > 65536) break;
                byte[] bytes = new byte[length]; in.readFully(bytes);
                try {
                    JSONObject request = new JSONObject(new String(bytes, StandardCharsets.UTF_8));
                    String op = request.getString("op");
                    JSONObject response = new JSONObject();
                    switch (op) {
                        case "capabilities":
                            synchronized (AppUseProjectionService.LOCK) { response.put("capture", AppUseProjectionService.target != null); }
                            response.put("input", !stopped).put("capture_protocol",2)
                                .put("capture_formats",new JSONArray(AppUseProjectionService.hasEncoder()?new String[]{"rgba8","bgra8","h264"}:new String[]{"rgba8","bgra8"})); break;
                        case "targets": {
                            JSONArray targets = new JSONArray();
                            synchronized (AppUseProjectionService.LOCK) {
                                String name = AppUseProjectionService.target;
                                if (name != null) targets.put(new JSONObject()
                                        .put("target", new JSONObject().put("kind", "monitor").put("device_name", name))
                                        .put("resource_id", "display:" + name).put("title", "Android shared display")
                                        .put("bounds", new JSONArray(new int[]{0, 0, AppUseProjectionService.width, AppUseProjectionService.height}))
                                        .put("client_bounds", JSONObject.NULL).put("minimized", false));
                            }
                            response.put("targets", targets); break;
                        }
                        case "validate": validate(request.getJSONObject("target")); break;
                        case "open_input":
                            if (input != null) throw new Failure("invalid", "input already open");
                            validate(request.getJSONObject("target")); input = new Input(request.getJSONObject("target"));
                            // The native input worker owns a 30-second idle lease.
                            // The short pre-handshake timeout must not end a valid
                            // session between actions or while a touch is held.
                            connection.setSoTimeout(35_000);
                            break;
                        case "input":
                            if (input == null) throw new Failure("invalid", "input is not open");
                            validate(input.target);
                            input.send(request.getJSONObject("event")); break;
                        case "release": if (input != null) input.release(); break;
                        case "open_capture":
                            if (capture) throw new Failure("invalid", "capture already open");
                            if(request.optInt("capture_protocol")!=2)throw new Failure("unsupported","capture protocol v2 required");
                            validate(request.getJSONObject("target"));
                            synchronized (AppUseProjectionService.LOCK) {
                                validate(request.getJSONObject("target"));
                                captureOwner = AppUseProjectionService.current;
                                AppUseProjectionService.clients++; capture = true;
                            }
                            captureClient=captureOwner.openCapture(request);
                            response.put("capture_protocol",2).put("width",captureClient.width).put("height",captureClient.height)
                                .put("format",captureClient.format).put("native_width",AppUseProjectionService.width).put("native_height",AppUseProjectionService.height);
                            break;
                        case "frame": {
                            if (!capture) throw new Failure("invalid", "capture is not open");
                            validate(request.getJSONObject("target"));
                            AppUseProjectionService.Frame frame;
                            long after = request.getLong("after");
                            long waitMs = Math.max(0, Math.min(100, request.optLong("wait_ms", 0)));
                            long deadline = android.os.SystemClock.elapsedRealtime() + waitMs;
                            frame=captureOwner.frame(captureClient,after);
                            while(frame==null && android.os.SystemClock.elapsedRealtime()<deadline) {
                                synchronized(AppUseProjectionService.LOCK) {
                                    validate(request.getJSONObject("target"));
                                    AppUseProjectionService.LOCK.wait(Math.max(1,Math.min(10,deadline-android.os.SystemClock.elapsedRealtime())));
                                }
                                frame=captureOwner.frame(captureClient,after);
                            }
                            validate(request.getJSONObject("target"));
                            if (frame == null || frame.sequence <= after) { response.put("available", false); break; }
                            response.put("available", true).put("format", frame.format).put("width", frame.width).put("height", frame.height)
                                    .put("native_width",frame.nativeWidth).put("native_height",frame.nativeHeight)
                                    .put("sequence", frame.sequence).put("timestamp_ns", frame.timestamp)
                                    .put("captured_at_unix_ms", frame.capturedAt).put("bytes", frame.bytes.length);
                            long writeStartedNs = android.os.SystemClock.elapsedRealtimeNanos();
                            respond(out, response); out.write(frame.bytes); out.flush();
                            if (android.util.Log.isLoggable("SinomoMediaStage", android.util.Log.VERBOSE)) {
                                long completedNs = android.os.SystemClock.elapsedRealtimeNanos();
                                android.util.Log.v("SinomoMediaStage", new JSONObject()
                                    .put("sequence", frame.sequence).put("captured_at_unix_ms", frame.capturedAt)
                                    .put("copy_us", frame.copyUs)
                                    .put("java_queue_us", (writeStartedNs - frame.publishedAtNs) / 1000)
                                    .put("socket_write_us", (completedNs - writeStartedNs) / 1000)
                                    .put("completed_unix_ms", System.currentTimeMillis())
                                    .put("bytes", frame.bytes.length).toString());
                            }
                            continue;
                        }
                        default: throw new Failure("unsupported", "unknown app-use operation");
                    }
                    respond(out, response);
                } catch (Exception error) {
                    if (input != null) input.release();
                    Throwable cause = error;
                    if (error instanceof java.util.concurrent.ExecutionException && error.getCause() != null) cause = error.getCause();
                    respond(out, new JSONObject().put("code", cause instanceof Failure ? ((Failure) cause).code : "platform")
                            .put("error", cause.getMessage() == null ? cause.toString() : cause.getMessage()));
                }
            }
        } catch (java.io.EOFException | java.net.SocketTimeoutException ignored) {
            // Connection loss ends this OS lease; requests are never retried.
        } catch (Exception error) {
            android.util.Log.w("SinomoAppUse", "Local OS bridge session failed", error);
        } finally {
            if (input != null) { input.release(); AppUseProjectionService.releaseInput(input.owner); }
            if (capture) { captureOwner.closeCapture(captureClient);AppUseProjectionService.releaseCapture(captureOwner); }
            connections.remove(socket); capacity.release();
        }
    }
    private final class Input {
        final JSONObject target;
        final AppUseProjectionService owner;
        float x, y;
        Path path;
        long downAt;
        int points;
        Input(JSONObject target) throws Exception {
            this.target = target;
            synchronized (AppUseProjectionService.LOCK) {
                validate(target);
                owner = AppUseProjectionService.current;
                AppUseProjectionService.inputClients++;
            }
        }
        void release() { path = null; points = 0; }
        void point(float px, float py) throws Exception {
            synchronized (AppUseProjectionService.LOCK) {
                if (px < 0 || py < 0 || px >= AppUseProjectionService.width || py >= AppUseProjectionService.height) throw new Failure("invalid", "touch point is outside the shared display");
            }
            x = px; y = py;
            if (path != null) {
                if (++points > 256) throw new Failure("invalid", "touch path exceeds 256 points");
                path.lineTo(x, y);
            }
        }
        void send(JSONObject event) throws Exception {
            switch (event.getString("kind")) {
                case "move": point(event.getInt("x"), event.getInt("y")); break;
                case "relative_move": point(x + event.getInt("dx"), y + event.getInt("dy")); break;
                case "button":
                    if (!"left".equals(event.getString("button"))) throw new Failure("unsupported", "Android supports left-button touch gestures only");
                    if (event.getBoolean("down")) {
                        if (path != null) throw new Failure("invalid", "touch is already down");
                        point(x, y); path = new Path(); path.moveTo(x, y); points = 1; downAt = android.os.SystemClock.uptimeMillis();
                    } else if (path != null) {
                        Path submitted = path;
                        long duration = Math.max(50, Math.min(1000, android.os.SystemClock.uptimeMillis() - downAt));
                        release(); gesture(submitted, duration);
                    }
                    break;
                case "wheel": {
                    int delta = event.getInt("delta");
                    if (delta == 0) break;
                    if (path != null) throw new Failure("invalid", "finish touch before scrolling");
                    boolean horizontal = event.optBoolean("horizontal");
                    int w, h;
                    synchronized (AppUseProjectionService.LOCK) { w = AppUseProjectionService.width; h = AppUseProjectionService.height; }
                    float cx = x, cy = y;
                    float distance = Math.copySign(Math.min(Math.abs((long) delta) * 0.5f, (horizontal ? w : h) / 3f), delta);
                    Path swipe = new Path(); swipe.moveTo(cx, cy);
                    swipe.lineTo(horizontal ? Math.max(0, Math.min(w - 1, cx + distance)) : cx,
                            horizontal ? cy : Math.max(0, Math.min(h - 1, cy + distance)));
                    gesture(swipe, 250); break;
                }
                case "text": edit(event.getString("text"), false); break;
                case "key":
                    int code = event.getInt("code");
                    if (event.optBoolean("extended") || (code != 1 && code != 14 && code != 28)) throw new Failure("unsupported", "Android raw keys support Escape/Back, Backspace and Enter only");
                    if (!event.getBoolean("down")) break;
                    if (code == 1) onMain(() -> { if (!performGlobalAction(GLOBAL_ACTION_BACK)) throw new Failure("platform", "Android rejected Back"); });
                    else if (code == 14) edit("", true);
                    else onMain(() -> {
                        AccessibilityNodeInfo node = focused();
                        try {
                            if (Build.VERSION.SDK_INT < 30 || !node.performAction(AccessibilityNodeInfo.AccessibilityAction.ACTION_IME_ENTER.getId())) throw new Failure("unsupported", "focused editor does not support IME Enter");
                        } finally { node.recycle(); }
                    });
                    break;
                case "release_all": release(); break;
                default: throw new Failure("unsupported", "Android does not support this input kind");
            }
        }
        void gesture(Path path, long duration) throws Exception {
            CountDownLatch done = new CountDownLatch(1);
            AtomicBoolean completed = new AtomicBoolean();
            onMain(() -> {
                GestureDescription gesture = new GestureDescription.Builder()
                        .addStroke(new GestureDescription.StrokeDescription(path, 0, duration)).build();
                if (!dispatchGesture(gesture, new GestureResultCallback() {
                    @Override public void onCompleted(GestureDescription g) { completed.set(true); done.countDown(); }
                    @Override public void onCancelled(GestureDescription g) { done.countDown(); }
                }, main)) throw new Failure("platform", "Android rejected the touch gesture");
            });
            if (!done.await(1500, TimeUnit.MILLISECONDS) || !completed.get()) throw new Failure("platform", "touch gesture cancelled or completion unknown; do not replay");
        }
        void edit(String text, boolean backspace) throws Exception {
            if (text.length() > 4096) throw new Failure("invalid", "text exceeds 4096 UTF-16 units");
            onMain(() -> {
                AccessibilityNodeInfo node = focused();
                try {
                    if (!node.isEditable() || node.isPassword()) throw new Failure("unsupported", "focused node must be a non-password accessible editor");
                    // Empty EditText nodes may expose their hint as text and
                    // omit selection entirely. A hint is not editable content.
                    String old = node.isShowingHintText() || node.getText() == null ? "" : node.getText().toString();
                    if (old.length() > 1_048_576) throw new Failure("unsupported", "focused editor is too large");
                    int start = node.getTextSelectionStart(), end = node.getTextSelectionEnd();
                    if (old.isEmpty() && start == -1 && end == -1) { start = 0; end = 0; }
                    if (start < 0 || end < start || end > old.length()) throw new Failure("unsupported", "focused editor has no valid selection");
                    if (backspace && start == end && start > 0) start = old.offsetByCodePoints(start, -1);
                    Bundle args = new Bundle();
                    args.putCharSequence(AccessibilityNodeInfo.ACTION_ARGUMENT_SET_TEXT_CHARSEQUENCE, old.substring(0, start) + text + old.substring(end));
                    if (!node.performAction(AccessibilityNodeInfo.ACTION_SET_TEXT, args)) throw new Failure("platform", "editor rejected text; do not replay");
                    Bundle selection = new Bundle();
                    selection.putInt(AccessibilityNodeInfo.ACTION_ARGUMENT_SELECTION_START_INT, start + text.length());
                    selection.putInt(AccessibilityNodeInfo.ACTION_ARGUMENT_SELECTION_END_INT, start + text.length());
                    node.performAction(AccessibilityNodeInfo.ACTION_SET_SELECTION, selection);
                } finally { node.recycle(); }
            });
        }
        AccessibilityNodeInfo focused() throws Failure {
            AccessibilityNodeInfo root = getRootInActiveWindow();
            if (root == null) throw new Failure("target_unavailable", "no accessible foreground window");
            try {
                AccessibilityNodeInfo node = root.findFocus(AccessibilityNodeInfo.FOCUS_INPUT);
                if (node == null) throw new Failure("unsupported", "no focused accessible editor");
                // Focus/selection can change without a window-state event. Read
                // the live node before editing rather than reusing cached text.
                if (!node.refresh()) { node.recycle(); throw new Failure("target_unavailable", "focused editor disappeared"); }
                return node;
            } finally { root.recycle(); }
        }
        void onMain(Action action) throws Exception {
            FutureTask<Void> task = new FutureTask<>(() -> { validate(target); action.run(); return null; });
            main.post(task);
            try { task.get(750, TimeUnit.MILLISECONDS); }
            finally { main.removeCallbacks(task); task.cancel(false); }
        }
    }
    private interface Action { void run() throws Exception; }
}
