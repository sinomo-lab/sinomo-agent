package net.sinomo.app.runtime;

import android.app.Activity;
import android.app.Application;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.graphics.Insets;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.NetworkCapabilities;
import android.os.Build;
import android.os.Bundle;
import android.os.PowerManager;
import android.view.DisplayCutout;
import android.view.View;
import android.view.ViewTreeObserver;
import android.view.WindowInsets;

/**
 * Bridges authoritative Activity lifecycle and window-inset transitions to
 * the Rust mobile runtime. The Application retains this callback after
 * start(), so no Java object needs to be held by Rust.
 */
public final class SinomoLifecycleCallbacks implements
        Application.ActivityLifecycleCallbacks,
        View.OnApplyWindowInsetsListener,
        ViewTreeObserver.OnWindowFocusChangeListener {
    public SinomoVideoEncoder openVideoEncoder(int width, int height, int fps) throws Exception {
        return new SinomoVideoEncoder(width, height, fps);
    }

    private final Activity activity;
    private boolean registered;
    private boolean observingOperatingConditions;
    private BroadcastReceiver operatingConditionsReceiver;
    private ConnectivityManager connectivityManager;
    private ConnectivityManager.NetworkCallback networkCallback;
    private PowerManager powerManager;
    private Object thermalStatusListener;

    public SinomoLifecycleCallbacks(Activity activity) {
        this.activity = activity;
    }

    public void start() {
        if (registered) {
            return;
        }
        activity.getApplication().registerActivityLifecycleCallbacks(this);
        View decor = activity.getWindow().getDecorView();
        decor.setOnApplyWindowInsetsListener(this);
        decor.getViewTreeObserver().addOnWindowFocusChangeListener(this);
        decor.requestApplyInsets();
        registered = true;

        // Registration can happen after onActivityResumed on some NativeActivity
        // startup paths. Focus is sufficient evidence that this Activity is
        // currently resumed; a false value remains conservative.
        if (activity.hasWindowFocus()) {
            startOperatingConditionsObservation();
            sinomoLifecycleChanged(true);
        }
    }

    private boolean isTarget(Activity candidate) {
        return candidate == activity;
    }

    @Override
    public WindowInsets onApplyWindowInsets(View view, WindowInsets windowInsets) {
        int left;
        int top;
        int right;
        int bottom;
        if (Build.VERSION.SDK_INT >= 30) {
            int types = WindowInsets.Type.systemBars() | WindowInsets.Type.displayCutout();
            Insets insets = windowInsets.getInsetsIgnoringVisibility(types);
            left = insets.left;
            top = insets.top;
            right = insets.right;
            bottom = insets.bottom;
            // Edge-to-edge NativeActivity surfaces do not resize for the IME.
            // Keep focused editors and their controls above the visible keyboard.
            bottom = Math.max(bottom, windowInsets.getInsets(WindowInsets.Type.ime()).bottom);
        } else {
            left = windowInsets.getStableInsetLeft();
            top = windowInsets.getStableInsetTop();
            right = windowInsets.getStableInsetRight();
            bottom = windowInsets.getStableInsetBottom();
            bottom = Math.max(bottom, windowInsets.getSystemWindowInsetBottom());
            if (Build.VERSION.SDK_INT >= 28) {
                DisplayCutout cutout = windowInsets.getDisplayCutout();
                if (cutout != null) {
                    left = Math.max(left, cutout.getSafeInsetLeft());
                    top = Math.max(top, cutout.getSafeInsetTop());
                    right = Math.max(right, cutout.getSafeInsetRight());
                    bottom = Math.max(bottom, cutout.getSafeInsetBottom());
                }
            }
        }
        sinomoWindowInsetsChanged(left, top, right, bottom,
                activity.getResources().getDisplayMetrics().density);
        return windowInsets;
    }

    @Override
    public void onWindowFocusChanged(boolean hasFocus) {
        // NativeActivity can finish onResume before this callback object is
        // installed. Window focus supplies the missing exact foreground edge
        // without keeping observers registered behind keyguard/system UI.
        if (hasFocus) {
            startOperatingConditionsObservation();
            sinomoLifecycleChanged(true);
        } else {
            stopOperatingConditionsObservation();
        }
    }

    @Override
    public void onActivityResumed(Activity candidate) {
        if (isTarget(candidate)) {
            startOperatingConditionsObservation();
            sinomoLifecycleChanged(true);
        }
    }

    @Override
    public void onActivityPaused(Activity candidate) {
        if (isTarget(candidate)) {
            sinomoLifecycleChanged(false);
            stopOperatingConditionsObservation();
        }
    }

    @Override
    public void onActivityStopped(Activity candidate) {
        if (isTarget(candidate)) {
            sinomoLifecycleChanged(false);
            stopOperatingConditionsObservation();
        }
    }

    @Override
    public void onActivityDestroyed(Activity candidate) {
        if (!isTarget(candidate)) {
            return;
        }
        sinomoLifecycleChanged(false);
        stopOperatingConditionsObservation();
        if (registered) {
            View decor = activity.getWindow().getDecorView();
            decor.setOnApplyWindowInsetsListener(null);
            ViewTreeObserver observer = decor.getViewTreeObserver();
            if (observer.isAlive()) {
                observer.removeOnWindowFocusChangeListener(this);
            }
            activity.getApplication().unregisterActivityLifecycleCallbacks(this);
            registered = false;
        }
    }

    @Override
    public void onActivityCreated(Activity candidate, Bundle state) {}

    @Override
    public void onActivityStarted(Activity candidate) {}

    @Override
    public void onActivitySaveInstanceState(Activity candidate, Bundle state) {}

    private void startOperatingConditionsObservation() {
        if (observingOperatingConditions) {
            return;
        }
        observingOperatingConditions = true;

        operatingConditionsReceiver = new BroadcastReceiver() {
            @Override
            public void onReceive(Context context, Intent intent) {
                notifyOperatingConditionsChanged();
            }
        };
        IntentFilter filter = new IntentFilter(Intent.ACTION_BATTERY_CHANGED);
        filter.addAction(PowerManager.ACTION_POWER_SAVE_MODE_CHANGED);
        filter.addAction(ConnectivityManager.CONNECTIVITY_ACTION);
        if (Build.VERSION.SDK_INT >= 24) {
            filter.addAction(ConnectivityManager.ACTION_RESTRICT_BACKGROUND_CHANGED);
        }
        try {
            if (Build.VERSION.SDK_INT >= 33) {
                activity.registerReceiver(
                        operatingConditionsReceiver,
                        filter,
                        Context.RECEIVER_NOT_EXPORTED);
            } else {
                activity.registerReceiver(operatingConditionsReceiver, filter);
            }
        } catch (RuntimeException error) {
            operatingConditionsReceiver = null;
        }

        connectivityManager = (ConnectivityManager) activity.getSystemService(
                Context.CONNECTIVITY_SERVICE);
        if (connectivityManager != null && Build.VERSION.SDK_INT >= 24) {
            networkCallback = new ConnectivityManager.NetworkCallback() {
                @Override
                public void onAvailable(Network network) {
                    notifyOperatingConditionsChanged();
                }

                @Override
                public void onLost(Network network) {
                    notifyOperatingConditionsChanged();
                }

                @Override
                public void onCapabilitiesChanged(
                        Network network,
                        NetworkCapabilities capabilities) {
                    notifyOperatingConditionsChanged();
                }

                @Override
                public void onBlockedStatusChanged(Network network, boolean blocked) {
                    notifyOperatingConditionsChanged();
                }
            };
            try {
                connectivityManager.registerDefaultNetworkCallback(networkCallback);
            } catch (RuntimeException error) {
                networkCallback = null;
            }
        }

        powerManager = (PowerManager) activity.getSystemService(Context.POWER_SERVICE);
        if (powerManager != null && Build.VERSION.SDK_INT >= 29) {
            thermalStatusListener = ThermalStatusObservation.register(
                    powerManager,
                    activity,
                    this);
        }
    }

    private void stopOperatingConditionsObservation() {
        if (!observingOperatingConditions) {
            return;
        }
        observingOperatingConditions = false;
        if (operatingConditionsReceiver != null) {
            try {
                activity.unregisterReceiver(operatingConditionsReceiver);
            } catch (RuntimeException ignored) {
                // The Activity can tear down receiver state before this callback.
            }
            operatingConditionsReceiver = null;
        }
        if (connectivityManager != null && networkCallback != null) {
            try {
                connectivityManager.unregisterNetworkCallback(networkCallback);
            } catch (RuntimeException ignored) {
                // A failed registration or vendor teardown can make this stale.
            }
            networkCallback = null;
        }
        if (powerManager != null && thermalStatusListener != null
                && Build.VERSION.SDK_INT >= 29) {
            ThermalStatusObservation.unregister(powerManager, thermalStatusListener);
            thermalStatusListener = null;
        }
        connectivityManager = null;
        powerManager = null;
    }

    private void notifyOperatingConditionsChanged() {
        if (observingOperatingConditions) {
            sinomoOperatingConditionsChanged();
        }
    }

    /** Keeps API-29-only thermal types out of the main class verifier on API 26-28. */
    private static final class ThermalStatusObservation {
        private static Object register(
                PowerManager powerManager,
                Activity activity,
                SinomoLifecycleCallbacks callback) {
            PowerManager.OnThermalStatusChangedListener listener =
                    status -> callback.notifyOperatingConditionsChanged();
            try {
                powerManager.addThermalStatusListener(activity.getMainExecutor(), listener);
                return listener;
            } catch (RuntimeException error) {
                return null;
            }
        }

        private static void unregister(PowerManager powerManager, Object listener) {
            try {
                powerManager.removeThermalStatusListener(
                        (PowerManager.OnThermalStatusChangedListener) listener);
            } catch (RuntimeException ignored) {
                // The listener may already have been removed during teardown.
            }
        }
    }

    public String readSensors() throws Exception {
        android.hardware.SensorManager manager = activity.getSystemService(android.hardware.SensorManager.class);
        if (manager == null) throw new IllegalStateException("Sensors unavailable");
        android.os.HandlerThread worker = new android.os.HandlerThread("sinomo-sensors");
        worker.start();
        final java.util.Map<Integer, org.json.JSONObject> samples = new java.util.TreeMap<>();
        final java.util.Set<Integer> requested = new java.util.HashSet<>();
        final java.util.concurrent.CountDownLatch sampleReady = new java.util.concurrent.CountDownLatch(1);
        android.hardware.SensorEventListener listener = new android.hardware.SensorEventListener() {
            @Override public void onAccuracyChanged(android.hardware.Sensor sensor, int accuracy) {}
            @Override public void onSensorChanged(android.hardware.SensorEvent event) {
                try {
                    org.json.JSONObject sample = new org.json.JSONObject();
                    sample.put("sensor", event.sensor.getName());
                    sample.put("sensor_type", event.sensor.getType());
                    sample.put("timestamp_ns", event.timestamp);
                    sample.put("accuracy", event.accuracy);
                    org.json.JSONArray values = new org.json.JSONArray();
                    for (float value : event.values) values.put((double)value);
                    sample.put("values", values);
                    synchronized (samples) {
                        samples.put(event.sensor.getType(), sample);
                        if (samples.keySet().containsAll(requested)) sampleReady.countDown();
                    }
                } catch (org.json.JSONException ignored) { /* Invalid samples are not published. */ }
            }
        };
        try {
            int[] types = { android.hardware.Sensor.TYPE_ACCELEROMETER, android.hardware.Sensor.TYPE_GYROSCOPE,
                android.hardware.Sensor.TYPE_MAGNETIC_FIELD, android.hardware.Sensor.TYPE_GRAVITY,
                android.hardware.Sensor.TYPE_ROTATION_VECTOR, android.hardware.Sensor.TYPE_LIGHT,
                android.hardware.Sensor.TYPE_PRESSURE };
            synchronized (samples) {
                for (int type : types) if (manager.getDefaultSensor(type) != null) requested.add(type);
                for (int type : types) {
                    android.hardware.Sensor sensor = manager.getDefaultSensor(type);
                    if (sensor != null && !manager.registerListener(listener, sensor, android.hardware.SensorManager.SENSOR_DELAY_NORMAL,
                            new android.os.Handler(worker.getLooper()))) requested.remove(type);
                }
            }
            if (requested.isEmpty()) throw new IllegalStateException("No supported sensors available");
            sampleReady.await(1, java.util.concurrent.TimeUnit.SECONDS);
            synchronized (samples) {
                if (samples.isEmpty()) throw new IllegalStateException("No sensor samples received");
                return new org.json.JSONArray(samples.values()).toString();
            }
        } finally { manager.unregisterListener(listener); worker.quitSafely(); }
    }

    public CameraStream openCamera(String selector, int width, int height) throws Exception {
        return new CameraStream(activity, selector, width, height);
    }

    public static final class CameraStream implements AutoCloseable {
        final android.os.HandlerThread thread = new android.os.HandlerThread("sinomo-camera2");
        final java.util.concurrent.CountDownLatch ready = new java.util.concurrent.CountDownLatch(1);
        android.hardware.camera2.CameraDevice camera;
        android.hardware.camera2.CameraCaptureSession session;
        android.media.ImageReader reader;
        int sensorOrientation;
        boolean frontFacing;
        byte[] latest;
        String failure;
        boolean closed;

        CameraStream(Context context, String selector, int requestedWidth, int requestedHeight) throws Exception {
            if (context.checkSelfPermission(android.Manifest.permission.CAMERA) != android.content.pm.PackageManager.PERMISSION_GRANTED)
                throw new SecurityException("Camera permission denied");
            thread.start();
            android.os.Handler handler = new android.os.Handler(thread.getLooper());
            try {
                android.hardware.camera2.CameraManager manager = context.getSystemService(android.hardware.camera2.CameraManager.class);
                String[] ids = manager.getCameraIdList();
                if (ids.length == 0) throw new IllegalStateException("No Android camera available");
                String id = ids[0];
                for (String candidate : ids) {
                    Integer facing = manager.getCameraCharacteristics(candidate).get(android.hardware.camera2.CameraCharacteristics.LENS_FACING);
                    if (facing != null && facing == android.hardware.camera2.CameraCharacteristics.LENS_FACING_FRONT) { id = candidate; break; }
                }
                if (selector != null && !selector.isEmpty() && !selector.equals("android:camera:default"))
                    throw new IllegalArgumentException("Unknown Android camera selector");
                android.hardware.camera2.CameraCharacteristics info = manager.getCameraCharacteristics(id);
                Integer orientation = info.get(android.hardware.camera2.CameraCharacteristics.SENSOR_ORIENTATION);
                sensorOrientation = orientation == null ? 0 : orientation;
                Integer facing = info.get(android.hardware.camera2.CameraCharacteristics.LENS_FACING);
                frontFacing = facing != null && facing == android.hardware.camera2.CameraCharacteristics.LENS_FACING_FRONT;
                android.hardware.camera2.params.StreamConfigurationMap map = info.get(android.hardware.camera2.CameraCharacteristics.SCALER_STREAM_CONFIGURATION_MAP);
                android.util.Size[] sizes = map.getOutputSizes(android.graphics.ImageFormat.YUV_420_888);
                android.util.Size chosen = null;
                long distance = Long.MAX_VALUE;
                for (android.util.Size size : sizes) {
                    if (size.getWidth() > 1920 || size.getHeight() > 1920 || size.getWidth() % 2 != 0 || size.getHeight() % 2 != 0) continue;
                    long delta = Math.abs((long)size.getWidth() * size.getHeight() - (long)requestedWidth * requestedHeight);
                    if (delta < distance) { chosen = size; distance = delta; }
                }
                if (chosen == null) throw new IllegalStateException("No bounded YUV camera size");
                reader = android.media.ImageReader.newInstance(chosen.getWidth(), chosen.getHeight(), android.graphics.ImageFormat.YUV_420_888, 2);
                reader.setOnImageAvailableListener(source -> {
                    try (android.media.Image image = source.acquireLatestImage()) {
                        if (image == null) return;
                        int w = image.getWidth(), h = image.getHeight();
                        byte[] packet = new byte[8 + w * h * 3 / 2];
                        java.nio.ByteBuffer.wrap(packet).putInt(w).putInt(h);
                        android.media.Image.Plane[] planes = image.getPlanes();
                        for (int p = 0; p < 3; p++) {
                            java.nio.ByteBuffer buffer = planes[p].getBuffer();
                            int rows = p == 0 ? h : h / 2, cols = p == 0 ? w : w / 2;
                            int base = buffer.position();
                            for (int y = 0; y < rows; y++) for (int x = 0; x < cols; x++) {
                                int output = p == 0 ? 8 + y * w + x : 8 + w * h + y * w + x * 2 + p - 1;
                                packet[output] = buffer.get(base + y * planes[p].getRowStride() + x * planes[p].getPixelStride());
                            }
                        }
                        int rotation = ((Activity)context).getWindowManager().getDefaultDisplay().getRotation() * 90;
                        int degrees = (sensorOrientation + (frontFacing ? rotation : -rotation) + 360) % 360;
                        packet = rotateNv12(packet, w, h, degrees);
                        synchronized (this) { if (!closed) { latest = packet; notifyAll(); } }
                    } catch (RuntimeException error) { fail(error.toString()); }
                }, handler);
                manager.openCamera(id, new android.hardware.camera2.CameraDevice.StateCallback() {
                    @Override public void onOpened(android.hardware.camera2.CameraDevice device) {
                        synchronized (CameraStream.this) {
                            if (closed) { device.close(); return; }
                            camera = device;
                        }
                        try {
                            device.createCaptureSession(java.util.Collections.singletonList(reader.getSurface()),
                                new android.hardware.camera2.CameraCaptureSession.StateCallback() {
                                    @Override public void onConfigured(android.hardware.camera2.CameraCaptureSession configured) {
                                        synchronized (CameraStream.this) {
                                            if (closed) { configured.close(); return; }
                                            session = configured;
                                        }
                                        try {
                                            android.hardware.camera2.CaptureRequest.Builder request = device.createCaptureRequest(android.hardware.camera2.CameraDevice.TEMPLATE_RECORD);
                                            request.addTarget(reader.getSurface());
                                            configured.setRepeatingRequest(request.build(), null, handler);
                                            ready.countDown();
                                        } catch (Exception error) { fail(error.toString()); }
                                    }
                                    @Override public void onConfigureFailed(android.hardware.camera2.CameraCaptureSession session) { fail("Camera configuration failed"); }
                                }, handler);
                        } catch (Exception error) { fail(error.toString()); }
                    }
                    @Override public void onDisconnected(android.hardware.camera2.CameraDevice device) { device.close(); fail("Camera disconnected"); }
                    @Override public void onError(android.hardware.camera2.CameraDevice device, int error) { device.close(); fail("Camera error " + error); }
                }, handler);
                if (!ready.await(10, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("Camera open timed out");
                synchronized (this) { if (failure != null) throw new IllegalStateException(failure); }
            } catch (Exception error) { close(); throw error; }
        }
        static byte[] rotateNv12(byte[] input, int width, int height, int degrees) {
            if (degrees == 0) return input;
            int outWidth = degrees == 180 ? width : height;
            int outHeight = degrees == 180 ? height : width;
            byte[] output = new byte[input.length];
            java.nio.ByteBuffer.wrap(output).putInt(outWidth).putInt(outHeight);
            for (int plane = 0; plane < 2; plane++) {
                int unit = plane == 0 ? 1 : 2;
                int w = plane == 0 ? width : width / 2;
                int h = plane == 0 ? height : height / 2;
                int ow = degrees == 180 ? w : h;
                int offset = plane == 0 ? 8 : 8 + width * height;
                for (int y = 0; y < h; y++) for (int x = 0; x < w; x++) {
                    int dx = degrees == 90 ? h - 1 - y : degrees == 180 ? w - 1 - x : y;
                    int dy = degrees == 90 ? x : degrees == 180 ? h - 1 - y : w - 1 - x;
                    for (int c = 0; c < unit; c++) output[offset + (dy * ow + dx) * unit + c] = input[offset + (y * w + x) * unit + c];
                }
            }
            return output;
        }
        synchronized void fail(String error) { failure = error; ready.countDown(); notifyAll(); }
        public synchronized byte[] next() throws Exception {
            if (latest == null && failure == null && !closed) wait(100);
            if (failure != null) throw new IllegalStateException(failure);
            if (closed) throw new IllegalStateException("Camera closed");
            byte[] packet = latest; latest = null; return packet;
        }
        @Override public synchronized void close() {
            if (closed) return;
            closed = true; latest = null; notifyAll();
            if (session != null) session.close();
            if (camera != null) camera.close();
            if (reader != null) reader.close();
            thread.quitSafely();
        }
    }

    /** Called only by a native worker, never by the UI thread. */
    public synchronized String mediaRequest(String operation) throws Exception {
        if (android.os.Looper.myLooper() == android.os.Looper.getMainLooper()) {
            throw new IllegalStateException("Media request must run on a worker");
        }
        MediaRequest request = new MediaRequest();
        request.operation = operation;
        activity.runOnUiThread(() -> {
            try {
                if (activity.isFinishing() || activity.isDestroyed()) {
                    request.complete("error:Activity is closed");
                    return;
                }
                activity.getFragmentManager().beginTransaction()
                        .add(request, "sinomo-media-" + System.identityHashCode(request))
                        .commitAllowingStateLoss();
            } catch (RuntimeException error) {
                request.complete("error:" + error.getMessage());
            }
        });
        if (!request.done.await(120, java.util.concurrent.TimeUnit.SECONDS)) {
            activity.runOnUiThread(() -> request.complete("error:Media request timed out"));
            return "error:Media request timed out";
        }
        return request.result;
    }

    /** A local UI request; this class is never reachable through cluster RPC. */
    public static final class MediaRequest extends android.app.Fragment {
        final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
        String operation;
        volatile String result = "error:Media request cancelled";
        boolean started;

        @Override public void onCreate(Bundle state) {
            super.onCreate(state);
            setRetainInstance(true);
        }

        @Override public void onResume() {
            super.onResume();
            if (started) return;
            started = true;
            if (operation == null) { complete("error:Media request interrupted"); return; }
            try {
                if (operation.equals("files") || operation.equals("file")) {
                    Intent intent = new Intent(Intent.ACTION_OPEN_DOCUMENT);
                    intent.addCategory(Intent.CATEGORY_OPENABLE);
                    intent.setType("*/*");
                    intent.putExtra(Intent.EXTRA_ALLOW_MULTIPLE, operation.equals("files"));
                    intent.addFlags(Intent.FLAG_GRANT_READ_URI_PERMISSION);
                    startActivityForResult(intent, 1);
                } else if (operation.equals("app-use")) {
                    Intent intent = new Intent();
                    intent.setClassName(getActivity(), "net.sinomo.app.use.AppUseConsentActivity");
                    startActivity(intent);
                    complete("ok");
                } else if (operation.equals("settings")) {
                    startActivity(new Intent(android.provider.Settings.ACTION_APPLICATION_DETAILS_SETTINGS,
                            android.net.Uri.parse("package:" + getActivity().getPackageName())));
                    complete("ok");
                } else {
                    String permission = operation.equals("microphone") ? android.Manifest.permission.RECORD_AUDIO
                            : operation.equals("camera") ? android.Manifest.permission.CAMERA : null;
                    if (permission == null) { complete("error:Unknown media request"); return; }
                    if (getActivity().checkSelfPermission(permission) == android.content.pm.PackageManager.PERMISSION_GRANTED) {
                        complete("ok");
                    } else {
                        requestPermissions(new String[] {permission}, 2);
                    }
                }
            } catch (RuntimeException error) { complete("error:" + error.getMessage()); }
        }

        @Override public void onRequestPermissionsResult(int code, String[] permissions, int[] grants) {
            if (code == 2) complete(grants.length == 1 && grants[0] == android.content.pm.PackageManager.PERMISSION_GRANTED
                    ? "ok" : "error:Permission denied; allow access in Android app settings and retry");
        }

        @Override public void onActivityResult(int code, int status, Intent data) {
            if (code != 1) return;
            if (status != Activity.RESULT_OK || data == null) { complete("cancelled"); return; }
            final Context context = getActivity().getApplicationContext();
            new Thread(() -> {
                java.util.ArrayList<java.io.File> copied = new java.util.ArrayList<>();
                try {
                    java.util.LinkedHashSet<android.net.Uri> uris = new java.util.LinkedHashSet<>();
                    if (data.getClipData() != null) {
                        if (data.getClipData().getItemCount() > 32) throw new java.io.IOException("Select at most 32 files");
                        for (int i = 0; i < data.getClipData().getItemCount(); i++) uris.add(data.getClipData().getItemAt(i).getUri());
                    } else if (data.getData() != null) uris.add(data.getData());
                    org.json.JSONArray paths = new org.json.JSONArray();
                    long total = 0;
                    for (android.net.Uri uri : uris) {
                        String name = "attachment";
                        try (android.database.Cursor cursor = context.getContentResolver().query(uri,
                                new String[] {android.provider.OpenableColumns.DISPLAY_NAME}, null, null, null)) {
                            if (cursor != null && cursor.moveToFirst() && !cursor.isNull(0)) name = cursor.getString(0);
                        }
                        name = name.replaceAll("[\\/\\\\\\p{Cntrl}]", "_");
                        if (name.equals(".") || name.equals("..") || name.isEmpty()) name = "attachment";
                        if (name.length() > 160) name = name.substring(name.length() - 160);
                        java.io.File directory = new java.io.File(context.getCacheDir(), "attachments/" + java.util.UUID.randomUUID());
                        if (!directory.mkdirs()) throw new java.io.IOException("Cannot create attachment cache");
                        java.io.File file = new java.io.File(directory, name);
                        copied.add(file);
                        try (java.io.InputStream input = context.getContentResolver().openInputStream(uri);
                             java.io.OutputStream output = new java.io.FileOutputStream(file)) {
                            if (input == null) throw new java.io.IOException("Selected document is unavailable");
                            byte[] buffer = new byte[65536];
                            int count;
                            while ((count = input.read(buffer)) != -1) {
                                total += count;
                                if (total > 128L * 1024 * 1024 || done.getCount() == 0) throw new java.io.IOException("Attachment import cancelled or exceeds 128 MiB");
                                output.write(buffer, 0, count);
                            }
                        }
                        paths.put(file.getAbsolutePath());
                    }
                    final String value = paths.toString();
                    new android.os.Handler(android.os.Looper.getMainLooper()).post(() -> complete(value));
                } catch (Exception error) {
                    for (java.io.File file : copied) { file.delete(); file.getParentFile().delete(); }
                    new android.os.Handler(android.os.Looper.getMainLooper()).post(() -> complete("error:" + error.getMessage()));
                }
            }, "sinomo-document-import").start();
        }

        void complete(String value) {
            if (done.getCount() == 0) return;
            result = value;
            done.countDown();
            if (isAdded()) getFragmentManager().beginTransaction().remove(this).commitAllowingStateLoss();
        }

        @Override public void onDestroy() {
            complete("error:Media request interrupted");
            super.onDestroy();
        }
    }

    private native void sinomoLifecycleChanged(boolean resumed);

    private native void sinomoWindowInsetsChanged(int left, int top, int right, int bottom, float density);

    private native void sinomoOperatingConditionsChanged();
}
