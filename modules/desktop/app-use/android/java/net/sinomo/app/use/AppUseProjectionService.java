package net.sinomo.app.use;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Intent;
import android.content.pm.ServiceInfo;
import android.hardware.display.DisplayManager;
import android.hardware.display.VirtualDisplay;
import android.media.projection.MediaProjection;
import android.media.projection.MediaProjectionManager;
import android.os.Build;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.SystemClock;
import android.util.DisplayMetrics;
import android.view.WindowManager;

/** MediaProjection owns one GPU texture; each consumer negotiates its output. */
public final class AppUseProjectionService extends Service {
    static final Object LOCK = new Object();
    private static final String STOP_ACTION="net.sinomo.app.media.STOP_SCREEN_SHARING";
    static volatile AppUseProjectionService current;
    static String target;
    static int width, height, rotation;
    private MediaProjection projection;
    private VirtualDisplay display;
    private AppUseGpuCapture gpu;
    private static volatile java.util.function.Function<String,Object> encoderFactory;
    public static void installEncoderFactory(java.util.function.Function<String,Object> factory) { encoderFactory=factory; }
    static boolean hasEncoder() { return encoderFactory!=null; }
    private HandlerThread thread;
    private Handler handler;
    private long geometryRevision;
    static long frameSignal;

    private volatile boolean closing;

    static final class Frame {
        final byte[] bytes;
        final int width, height, nativeWidth, nativeHeight;
        final String format;
        final long sequence, timestamp, capturedAt, copyUs, publishedAtNs;
        Frame(byte[] bytes, int width, int height, int nativeWidth, int nativeHeight, String format, long sequence, long timestamp, long capturedAt, long copyUs) {
            this.nativeWidth=nativeWidth;this.nativeHeight=nativeHeight;this.format=format;
            this.bytes = bytes; this.width = width; this.height = height;
            this.sequence = sequence; this.timestamp = timestamp;
            this.capturedAt = capturedAt;
            this.copyUs = copyUs; this.publishedAtNs = SystemClock.elapsedRealtimeNanos();
        }
    }
    @Override public IBinder onBind(Intent intent) { return null; }
    @Override public int onStartCommand(Intent intent, int flags, int id) {
        if(intent!=null && STOP_ACTION.equals(intent.getAction())) { stopSelf();return START_NOT_STICKY; }
        // Repeated starts cannot revoke the existing user-approved projection.
        // Android consent tokens are never cached or replayed after termination.
        if (projection != null) return START_NOT_STICKY;
        if (intent == null || !intent.hasExtra("consent")) { stopSelf();return START_NOT_STICKY; }
        try {
            NotificationManager notifications = getSystemService(NotificationManager.class);
            notifications.createNotificationChannel(new NotificationChannel("app-use", "App Use screen sharing", NotificationManager.IMPORTANCE_LOW));
            PendingIntent controls = PendingIntent.getActivity(this, 0,
                    new Intent(this, AppUseConsentActivity.class).addFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP),
                    PendingIntent.FLAG_IMMUTABLE);
            PendingIntent stop = PendingIntent.getService(this, 1,
                    new Intent(this, AppUseProjectionService.class).setAction(STOP_ACTION), PendingIntent.FLAG_IMMUTABLE);
            Notification notification = new Notification.Builder(this, "app-use")
                    .setContentTitle("Sinomo App Use is sharing your screen")
                    .setContentText("Tap to manage sharing").setSmallIcon(android.R.drawable.ic_menu_view)
                    .addAction(android.R.drawable.ic_menu_close_clear_cancel,"Stop sharing",stop)
                    .setContentIntent(controls).setOngoing(true).build();
            if (Build.VERSION.SDK_INT >= 29) startForeground(41, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PROJECTION);
            else startForeground(41, notification);
            thread = new HandlerThread("app-use-projection"); thread.start(); handler = new Handler(thread.getLooper());
            projection = getSystemService(MediaProjectionManager.class).getMediaProjection(
                    intent.getIntExtra("result", 0), intent.getParcelableExtra("consent"));
            projection.registerCallback(new MediaProjection.Callback() {
                @Override public void onStop() { stopSelf(); }
                @Override public void onCapturedContentResize(int w, int h) {
                    resizeCapturedContent(w,h);
                }
            }, handler);
            DisplayMetrics metrics = new DisplayMetrics();
            getSystemService(WindowManager.class).getDefaultDisplay().getRealMetrics(metrics);
            if (metrics.widthPixels <= 0 || metrics.heightPixels <= 0 || (long) metrics.widthPixels * metrics.heightPixels * 4 > 128 * 1024 * 1024) throw new IllegalArgumentException("display is too large");
            synchronized (LOCK) {
                width = metrics.widthPixels; height = metrics.heightPixels;
                rotation = getSystemService(WindowManager.class).getDefaultDisplay().getRotation();
                target = "android:display:0:" + java.util.UUID.randomUUID();
                current = this;
            }
            AppUseBridge.start(this);
            gpu = invoke(() -> new AppUseGpuCapture(width,height,handler,() -> {
                synchronized(LOCK) { frameSignal++;LOCK.notifyAll(); }
            },() -> stopSelf()));
            display = projection.createVirtualDisplay("Sinomo App Use", width, height, metrics.densityDpi,
                    DisplayManager.VIRTUAL_DISPLAY_FLAG_AUTO_MIRROR, gpu.surface, null, handler);
            AppUseConsentActivity.serviceStateChanged();
            // The explicit sharing session owns projection lifetime, independently
            // of transient readers, input leases and the foreground application.
        } catch (Exception error) {
            android.util.Log.e("SinomoAppUse", "Unable to start screen sharing", error);
            stopSelf();
        }
        return START_NOT_STICKY;
    }
    @Override public void onConfigurationChanged(android.content.res.Configuration configuration) {
        super.onConfigurationChanged(configuration);
        if(Build.VERSION.SDK_INT<34 && handler!=null)handler.post(() -> {
            DisplayMetrics metrics=new DisplayMetrics();getSystemService(WindowManager.class).getDefaultDisplay().getRealMetrics(metrics);
            resizeCapturedContent(metrics.widthPixels,metrics.heightPixels);
        });
    }
    private void resizeCapturedContent(int w,int h) {
        if(closing)return;
        // Hidden/transitional content can temporarily have no drawable extent.
        // Keep the approved session and wait for usable geometry.
        if(w<=0||h<=0)return;
        if((long)w*h>32*1024*1024) { android.util.Log.e("SinomoAppUse","Projection geometry exceeds capture capacity");stopSelf();return; }
        synchronized(LOCK) { if(w==width && h==height)return; }
        try {
            // Keep the same MediaProjection token and VirtualDisplay. Consumer
            // output sizes remain fixed; the renderer letterboxes the new source.
            display.setSurface(null);
            gpu.resize(w,h);
            display.resize(w,h,getResources().getDisplayMetrics().densityDpi);
            display.setSurface(gpu.surface);
            synchronized(LOCK) {
                width=w;height=h;rotation=getSystemService(WindowManager.class).getDefaultDisplay().getRotation();
                geometryRevision++;frameSignal++;LOCK.notifyAll();
            }
            sendBroadcast(new Intent("net.sinomo.app.media.SCREEN_GEOMETRY_CHANGED").setPackage(getPackageName())
                .putExtra("kind","media.video.geometry.changed").putExtra("source",target)
                .putExtra("width",w).putExtra("height",h).putExtra("rotation",rotation).putExtra("revision",geometryRevision));
        } catch(Exception error) { android.util.Log.e("SinomoAppUse","Projection resize failed",error);stopSelf(); }
    }
    private <T> T invoke(java.util.concurrent.Callable<T> operation) throws Exception {
        if(android.os.Looper.myLooper()==handler.getLooper())return operation.call();
        java.util.concurrent.FutureTask<T> task=new java.util.concurrent.FutureTask<>(operation);
        if(!handler.post(task))throw new IllegalStateException("projection thread stopped");
        // The socket timeout bounds the native caller. Let this operation finish
        // so serve() retains ownership of any newly allocated client and closes
        // it when the disconnected socket is observed; abandoning the Future
        // here could orphan a codec Surface on a slow driver initialization.
        return task.get();
    }
    AppUseGpuCapture.Client openCapture(org.json.JSONObject request) throws Exception {
        return invoke(() -> {if(closing||gpu==null)throw new IllegalStateException("projection closed");return gpu.open(request,encoderFactory);});
    }
    Frame frame(AppUseGpuCapture.Client client,long after) throws Exception { return invoke(() -> client.frame(after)); }
    long readyDelayMillis(AppUseGpuCapture.Client client,long after,long maximum) throws Exception {
        return invoke(() -> client.readyDelayMillis(after,maximum));
    }
    void closeCapture(AppUseGpuCapture.Client client) {
        if(client!=null&&handler!=null)handler.post(client::close);
    }
    @Override public void onDestroy() {
        synchronized (LOCK) { closing = true; if (current == this) { current = null; target = null; LOCK.notifyAll(); } }
        AppUseConsentActivity.serviceStateChanged();
        if (display != null) display.release();
        if (gpu != null) { try { invoke(() -> { gpu.close();return null; }); } catch(Exception error) { android.util.Log.w("SinomoAppUse","GPU shutdown failed",error); } }
        if (projection != null) projection.stop();
        if (thread != null) {
            thread.quitSafely();
            try { thread.join(2000); } catch (InterruptedException error) { Thread.currentThread().interrupt(); }
        }
        AppUseBridge.stopIfUnused();
        super.onDestroy();
    }
}
