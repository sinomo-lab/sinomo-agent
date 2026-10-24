package net.sinomo.app.use;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.graphics.PixelFormat;
import android.hardware.display.DisplayManager;
import android.hardware.display.VirtualDisplay;
import android.media.Image;
import android.media.ImageReader;
import android.media.projection.MediaProjection;
import android.media.projection.MediaProjectionManager;
import android.os.Build;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.SystemClock;
import android.util.DisplayMetrics;
import android.view.WindowManager;
import java.nio.ByteBuffer;

/** MediaProjection pushes frames into one bounded slot; Rust selects cadence. */
public final class AppUseProjectionService extends Service {
    static final Object LOCK = new Object();
    static AppUseProjectionService current;
    static Frame latest;
    static String target;
    static int width, height, rotation, clients, inputClients;
    static boolean frameRequested;
    private MediaProjection projection;
    private VirtualDisplay display;
    private ImageReader reader;
    private HandlerThread thread;
    private Handler handler;
    private long sequence, started;
    private boolean closing;

    static final class Frame {
        final byte[] bytes;
        final int width, height;
        final long sequence, timestamp, capturedAt;
        Frame(byte[] bytes, int width, int height, long sequence, long timestamp) {
            this.bytes = bytes; this.width = width; this.height = height;
            this.sequence = sequence; this.timestamp = timestamp;
            this.capturedAt = System.currentTimeMillis();
        }
    }
    @Override public IBinder onBind(Intent intent) { return null; }
    @Override public int onStartCommand(Intent intent, int flags, int id) {
        // Each OS token can be used only once. Reopening starts a fresh service.
        if (projection != null || intent == null || !intent.hasExtra("consent")) {
            stopSelf(); return START_NOT_STICKY;
        }
        try {
            NotificationManager notifications = getSystemService(NotificationManager.class);
            notifications.createNotificationChannel(new NotificationChannel("app-use", "App Use screen sharing", NotificationManager.IMPORTANCE_LOW));
            PendingIntent controls = PendingIntent.getActivity(this, 0,
                    new Intent(this, AppUseConsentActivity.class), PendingIntent.FLAG_IMMUTABLE);
            Notification notification = new Notification.Builder(this, "app-use")
                    .setContentTitle("Sinomo App Use is sharing your screen")
                    .setContentText("Tap to stop sharing").setSmallIcon(android.R.drawable.ic_menu_view)
                    .setContentIntent(controls).setOngoing(true).build();
            if (Build.VERSION.SDK_INT >= 29) startForeground(41, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PROJECTION);
            else startForeground(41, notification);
            thread = new HandlerThread("app-use-projection"); thread.start(); handler = new Handler(thread.getLooper());
            projection = getSystemService(MediaProjectionManager.class).getMediaProjection(
                    intent.getIntExtra("result", 0), intent.getParcelableExtra("consent"));
            started = SystemClock.elapsedRealtimeNanos();
            projection.registerCallback(new MediaProjection.Callback() {
                @Override public void onStop() { stopSelf(); }
                @Override public void onCapturedContentResize(int w, int h) {
                    synchronized (LOCK) {
                        if (w != width || h != height) {
                            android.util.Log.i("SinomoAppUse", "Shared content geometry changed from " + width + "x" + height + " to " + w + "x" + h);
                            // Rotation/app-only selection invalidates coordinates. Do
                            // not keep input alive against a stale raw display grant.
                            target = null; latest = null; stopSelf();
                        }
                    }
                }
            }, handler);
            DisplayMetrics metrics = new DisplayMetrics();
            getSystemService(WindowManager.class).getDefaultDisplay().getRealMetrics(metrics);
            if (metrics.widthPixels <= 0 || metrics.heightPixels <= 0 || (long) metrics.widthPixels * metrics.heightPixels * 4 > 128 * 1024 * 1024) throw new IllegalArgumentException("display is too large");
            synchronized (LOCK) {
                width = metrics.widthPixels; height = metrics.heightPixels; clients = 0; inputClients = 0; latest = null; frameRequested = false;
                rotation = getSystemService(WindowManager.class).getDefaultDisplay().getRotation();
                target = "android:display:0:" + java.util.UUID.randomUUID() + ":" + width + "x" + height;
                current = this;
            }
            reader = ImageReader.newInstance(width, height, PixelFormat.RGBA_8888, 2);
            reader.setOnImageAvailableListener(source -> receive(source), handler);
            display = projection.createVirtualDisplay("Sinomo App Use", width, height, metrics.densityDpi,
                    DisplayManager.VIRTUAL_DISPLAY_FLAG_AUTO_MIRROR, reader.getSurface(), null, handler);
            // Consent cannot leave an unattended projection running indefinitely.
            handler.postDelayed(() -> { synchronized (LOCK) { if (clients == 0 && inputClients == 0) stopSelf(); } }, 30_000);
        } catch (RuntimeException error) {
            android.util.Log.e("SinomoAppUse", "Unable to start screen sharing", error);
            stopSelf();
        }
        return START_NOT_STICKY;
    }
    private void receive(ImageReader source) {
        try (Image image = source.acquireLatestImage()) {
            if (image == null) return;
            synchronized (LOCK) {
                if (closing || clients == 0 || target == null || !frameRequested) return;
                // Drain OS frames continuously but copy only on consumer demand.
                // Multiple requests coalesce instead of creating a frame queue.
                frameRequested = false;
            }
            int w = image.getWidth(), h = image.getHeight();
            Image.Plane plane = image.getPlanes()[0];
            ByteBuffer buffer = plane.getBuffer();
            int row = plane.getRowStride(), pixel = plane.getPixelStride();
            if (pixel < 4 || w <= 0 || h <= 0 || (long) w * h * 4 > 128 * 1024 * 1024
                    || (long) (h - 1) * row + (long) (w - 1) * pixel + 4 > buffer.limit()) return;
            byte[] bytes = new byte[w * h * 4];
            for (int y = 0; y < h; y++) {
                if (pixel == 4) { buffer.position(y * row); buffer.get(bytes, y * w * 4, w * 4); }
                else for (int x = 0; x < w; x++) {
                    int src = y * row + x * pixel, dst = (y * w + x) * 4;
                    for (int channel = 0; channel < 4; channel++) bytes[dst + channel] = buffer.get(src + channel);
                }
            }
            synchronized (LOCK) {
                if (!closing && clients > 0 && target != null && width == w && height == h)
                    latest = new Frame(bytes, w, h, ++sequence, SystemClock.elapsedRealtimeNanos() - started);
            }
        } catch (RuntimeException error) {
            android.util.Log.e("SinomoAppUse", "Native frame delivery failed", error);
            stopSelf();
        }
    }
    static void releaseCapture(AppUseProjectionService owner) {
        synchronized (LOCK) {
            if (owner != current) return;
            clients = Math.max(0, clients - 1);
            if (clients == 0) latest = null;
            if (clients == 0 && inputClients == 0 && current != null) current.stopSelf();
        }
    }
    static void releaseInput(AppUseProjectionService owner) {
        synchronized (LOCK) {
            if (owner != current) return;
            inputClients = Math.max(0, inputClients - 1);
            if (clients == 0 && inputClients == 0 && current != null) current.stopSelf();
        }
    }
    @Override public void onDestroy() {
        synchronized (LOCK) { closing = true; if (current == this) { current = null; target = null; latest = null; clients = 0; inputClients = 0; } }
        if (reader != null) reader.setOnImageAvailableListener(null, null);
        if (display != null) display.release();
        if (reader != null) reader.close();
        if (projection != null) projection.stop();
        if (thread != null) {
            thread.quitSafely();
            try { thread.join(2000); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
        }
        super.onDestroy();
    }
}
