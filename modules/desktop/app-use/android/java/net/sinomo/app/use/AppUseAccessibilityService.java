package net.sinomo.app.use;

import android.accessibilityservice.AccessibilityService;
import android.graphics.Bitmap;
import android.graphics.Rect;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.SystemClock;
import android.view.Display;
import android.view.WindowManager;
import android.view.accessibility.AccessibilityEvent;
import android.view.accessibility.AccessibilityNodeInfo;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.json.JSONArray;
import org.json.JSONObject;

/** Persistent, user-enabled observation/control. Projection is an independent service. */
public final class AppUseAccessibilityService extends AccessibilityService {
    static final String TARGET="android:accessibility:display:0";
    static volatile AppUseAccessibilityService current;
    private final Handler main=new Handler(Looper.getMainLooper());
    private final LinkedHashMap<String,Snapshot> snapshots=new LinkedHashMap<>();
    private long nextScreenshot, sequence;
    private final java.util.concurrent.ExecutorService screenshotWorker=java.util.concurrent.Executors.newSingleThreadExecutor();

    static boolean canScreenshot() {
        AppUseAccessibilityService service=current;
        if(Build.VERSION.SDK_INT<30||service==null)return false;
        android.accessibilityservice.AccessibilityServiceInfo info=service.getServiceInfo();
        return info!=null && (info.getCapabilities() & android.accessibilityservice.AccessibilityServiceInfo.CAPABILITY_CAN_TAKE_SCREENSHOT)!=0;
    }
    @Override protected void onServiceConnected() {
        current=this;
        try { AppUseBridge.start(this); }
        catch(Exception error) { android.util.Log.e("SinomoAppUse","Accessibility bridge unavailable",error); }
    }
    @Override public void onAccessibilityEvent(AccessibilityEvent event) {
        // An observation is tied to its window; app switches cannot retarget a
        // pending semantic action. Content is also refreshed at the effect edge.
        if(event.getEventType()==AccessibilityEvent.TYPE_WINDOW_STATE_CHANGED
                || event.getEventType()==AccessibilityEvent.TYPE_WINDOWS_CHANGED) clearSnapshots();
    }
    @Override public void onInterrupt() { clearSnapshots(); }
    @Override public void onDestroy() {
        if(current==this)current=null;
        clearSnapshots();screenshotWorker.shutdown();AppUseBridge.stopIfUnused();super.onDestroy();
    }
    private void clearSnapshots() { for(Snapshot snapshot:snapshots.values())snapshot.close();snapshots.clear(); }
    private <T> T onMain(java.util.concurrent.Callable<T> action) throws Exception {
        FutureTask<T> task=new FutureTask<>(() -> {
            if(current!=this)throw new AppUseBridge.Failure("permission_denied","Accessibility is disabled");
            return action.call();
        });
        main.post(task);
        try { return task.get(1500,TimeUnit.MILLISECONDS); }
        finally { main.removeCallbacks(task);task.cancel(false); }
    }
    private static String text(CharSequence value) {
        String text=value==null?"":value.toString();return text.substring(0,Math.min(256,text.length()));
    }
    private static String fingerprint(AccessibilityNodeInfo node) {
        Rect bounds=new Rect();node.getBoundsInScreen(bounds);
        try {
            java.security.MessageDigest digest=java.security.MessageDigest.getInstance("SHA-256");
            String state=node.getWindowId()+":"+node.isEnabled()+":"+node.isVisibleToUser()+":"+node.isEditable()+":"+node.isClickable()+":"+bounds;
            for(CharSequence value:new CharSequence[]{state,node.getPackageName(),node.getViewIdResourceName(),node.getClassName(),
                    node.isPassword()?"":node.getText(),node.isPassword()?"":node.getContentDescription()}) {
                byte[] bytes=(value==null?"":value.toString()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array());digest.update(bytes);
            }
            return java.util.Base64.getEncoder().encodeToString(digest.digest());
        } catch(java.security.NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}
    }
    private static final class Snapshot {
        final long expires=SystemClock.uptimeMillis()+30_000;
        final Map<String,AccessibilityNodeInfo> nodes=new LinkedHashMap<>();
        final Map<String,String> fingerprints=new LinkedHashMap<>();
        void close(){for(AccessibilityNodeInfo node:nodes.values())node.recycle();nodes.clear();}
    }
    private static final class Pending {
        final AccessibilityNodeInfo node;final String parent;final int depth;
        Pending(AccessibilityNodeInfo node,String parent,int depth){this.node=node;this.parent=parent;this.depth=depth;}
    }
    JSONObject observe(int maxNodes) throws Exception {
        if(maxNodes<1||maxNodes>128)throw new AppUseBridge.Failure("invalid","max_nodes must be 1..128");
        return onMain(() -> {
            AccessibilityNodeInfo root=getRootInActiveWindow();
            if(root==null)throw new AppUseBridge.Failure("target_unavailable","no accessible foreground window");
            Snapshot snapshot=new Snapshot();JSONArray nodes=new JSONArray();
            ArrayDeque<Pending> pending=new ArrayDeque<>();pending.add(new Pending(root,null,0));
            boolean truncated=false;int bytes=0;
            try {
                while(!pending.isEmpty()) {
                    Pending item=pending.removeFirst();AccessibilityNodeInfo node=item.node;
                    if(nodes.length()>=maxNodes||item.depth>32){node.recycle();truncated=true;continue;}
                    if(!node.refresh()){node.recycle();truncated=true;continue;}
                    String id=Integer.toString(nodes.length()+1);Rect bounds=new Rect();node.getBoundsInScreen(bounds);
                    boolean password=node.isPassword();
                    JSONObject value=new JSONObject().put("id",id).put("parent",item.parent==null?JSONObject.NULL:item.parent)
                        .put("window_id",node.getWindowId()).put("package",text(node.getPackageName()))
                        .put("class_name",text(node.getClassName())).put("view_id",text(node.getViewIdResourceName()))
                        .put("text",password?"":text(node.getText())).put("description",password?"":text(node.getContentDescription()))
                        .put("bounds",new JSONArray(new int[]{bounds.left,bounds.top,bounds.right,bounds.bottom}))
                        .put("enabled",node.isEnabled()).put("clickable",node.isClickable()).put("editable",node.isEditable())
                        .put("scrollable",node.isScrollable()).put("password",password);
                    int size=value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
                    if(bytes+size>48_000){node.recycle();truncated=true;continue;}
                    bytes+=size;nodes.put(value);snapshot.nodes.put(id,node);snapshot.fingerprints.put(id,fingerprint(node));
                    if(!password)for(int i=0;i<node.getChildCount();i++) {
                        if(pending.size()+nodes.length()>=maxNodes){truncated=true;break;}
                        AccessibilityNodeInfo child=node.getChild(i);if(child!=null)pending.add(new Pending(child,id,item.depth+1));
                    }
                }
                while(snapshots.size()>=8){String first=snapshots.keySet().iterator().next();snapshots.remove(first).close();}
                String id=UUID.randomUUID().toString();snapshots.put(id,snapshot);
                int[] size=geometry();
                return new JSONObject().put("snapshot_id",id).put("captured_at_unix_ms",System.currentTimeMillis())
                    .put("width",size[0]).put("height",size[1]).put("nodes",nodes).put("truncated",truncated);
            } catch(Exception error){snapshot.close();throw error;}
            finally {for(Pending item:pending)item.node.recycle();}
        });
    }
    void act(String snapshotId,String nodeId,JSONObject action) throws Exception {
        onMain(() -> {
            if(AppUseConsentActivity.protectsLocalConsent())throw new AppUseBridge.Failure("permission_denied","App Use cannot operate its own local consent controls");
            Snapshot snapshot=snapshots.get(snapshotId);
            if(snapshot==null||SystemClock.uptimeMillis()>=snapshot.expires)throw new AppUseBridge.Failure("target_unavailable","observation expired; observe again");
            AccessibilityNodeInfo node=snapshot.nodes.get(nodeId);
            if(node==null||!node.refresh()||!fingerprint(node).equals(snapshot.fingerprints.get(nodeId)))
                throw new AppUseBridge.Failure("target_unavailable","observed node changed; observe again");
            if(node.isPassword()||!node.isEnabled()||!node.isVisibleToUser())throw new AppUseBridge.Failure("permission_denied","protected, disabled or hidden node");
            int operation;Bundle arguments=new Bundle();
            switch(action.getString("kind")) {
                case "click":operation=AccessibilityNodeInfo.ACTION_CLICK;break;
                case "long_click":operation=AccessibilityNodeInfo.ACTION_LONG_CLICK;break;
                case "focus":operation=AccessibilityNodeInfo.ACTION_FOCUS;break;
                case "scroll_forward":operation=AccessibilityNodeInfo.ACTION_SCROLL_FORWARD;break;
                case "scroll_backward":operation=AccessibilityNodeInfo.ACTION_SCROLL_BACKWARD;break;
                case "set_text":
                    if(!node.isEditable())throw new AppUseBridge.Failure("unsupported","node is not editable");
                    String value=action.getString("text");if(value.length()>4096)throw new AppUseBridge.Failure("invalid","text exceeds 4096 UTF-16 units");
                    operation=AccessibilityNodeInfo.ACTION_SET_TEXT;arguments.putCharSequence(AccessibilityNodeInfo.ACTION_ARGUMENT_SET_TEXT_CHARSEQUENCE,value);break;
                default:throw new AppUseBridge.Failure("unsupported","unknown accessibility action");
            }
            // Consume before effect: an ambiguous timeout must never replay it.
            snapshots.remove(snapshotId);
            try {if(!node.performAction(operation,arguments))throw new AppUseBridge.Failure("platform","action rejected; observe before retrying");}
            finally {snapshot.close();}
            return null;
        });
    }
    private int[] geometry() {
        android.util.DisplayMetrics metrics=new android.util.DisplayMetrics();
        getSystemService(WindowManager.class).getDefaultDisplay().getRealMetrics(metrics);
        return new int[]{metrics.widthPixels,metrics.heightPixels};
    }
    final class ScreenshotCapture {
        final String format;final int boundWidth,boundHeight;
        private final FrameRateCeiling ceiling;
        ScreenshotCapture(JSONObject request) throws Exception {
            ceiling=new FrameRateCeiling(request.getInt("fps"));
            if(!canScreenshot())throw new AppUseBridge.Failure("unsupported","Accessibility screenshots require Android 11 and the screenshot capability in the enabled service");
            format=request.optString("pixel_format","rgba8").equals("bgra8")?"bgra8":"rgba8";
            if(request.optString("pixel_format").equals("h264"))throw new AppUseBridge.Failure("unsupported","Accessibility provides pixels; use generic screen video for encoder surfaces");
            JSONObject bounds=request.optJSONObject("resolution");int[] size=geometry();
            boundWidth=bounds==null?size[0]:bounds.getInt("width");boundHeight=bounds==null?size[1]:bounds.getInt("height");
            if(boundWidth<1||boundHeight<1||boundWidth>8192||boundHeight>8192||(long)boundWidth*boundHeight>32*1024*1024)
                throw new AppUseBridge.Failure("invalid","invalid screenshot bounds");
        }
        long readyDelayMillis(long maximum) {
            synchronized(AppUseAccessibilityService.this) {
                long delay=Math.max(ceiling.delayNanos(System.nanoTime()),Math.max(0,nextScreenshot-SystemClock.uptimeMillis())*1_000_000L);
                return Math.min(maximum,(delay+999_999)/1_000_000);
            }
        }
        AppUseProjectionService.Frame frame(long after) throws Exception {
            if(current!=AppUseAccessibilityService.this)throw new AppUseBridge.Failure("permission_denied","Accessibility is disabled");
            synchronized(AppUseAccessibilityService.this) {
                long now=SystemClock.uptimeMillis();long nanos=System.nanoTime();
                if(now<nextScreenshot||ceiling.delayNanos(nanos)>0)return null;
                nextScreenshot=now+350;ceiling.admit(nanos);
            }
            CompletableFuture<AppUseProjectionService.Frame> result=new CompletableFuture<>();
            long started=System.nanoTime();
            takeScreenshot(Display.DEFAULT_DISPLAY,command -> {
                try { screenshotWorker.execute(command); }
                catch(java.util.concurrent.RejectedExecutionException stopped){command.run();}
            },new TakeScreenshotCallback() {
                @Override public void onFailure(int error) {
                    if(error==ERROR_TAKE_SCREENSHOT_INTERVAL_TIME_SHORT)result.complete(null);
                    else result.completeExceptionally(new AppUseBridge.Failure("target_unavailable","Accessibility screenshot failed: "+error));
                }
                @Override public void onSuccess(ScreenshotResult shot) {
                    Bitmap hardware=null,pixels=null,scaled=null;
                    try {
                        if(current!=AppUseAccessibilityService.this)throw new AppUseBridge.Failure("permission_denied","Accessibility was disabled");
                        hardware=Bitmap.wrapHardwareBuffer(shot.getHardwareBuffer(),shot.getColorSpace());
                        if(hardware==null||(long)hardware.getWidth()*hardware.getHeight()>32*1024*1024)throw new IllegalStateException("invalid screenshot size");
                        int nativeWidth=hardware.getWidth(),nativeHeight=hardware.getHeight();
                        double scale=Math.min(1,Math.min((double)boundWidth/nativeWidth,(double)boundHeight/nativeHeight));
                        int width=Math.max(1,(int)(nativeWidth*scale)),height=Math.max(1,(int)(nativeHeight*scale));
                        pixels=hardware.copy(Bitmap.Config.ARGB_8888,false);scaled=Bitmap.createScaledBitmap(pixels,width,height,true);
                        int[] colors=new int[width*height];scaled.getPixels(colors,0,width,0,0,width,height);
                        byte[] bytes=new byte[width*height*4];boolean bgra=format.equals("bgra8");
                        for(int i=0;i<colors.length;i++){int color=colors[i];bytes[i*4]=(byte)(bgra?color:color>>16);bytes[i*4+1]=(byte)(color>>8);bytes[i*4+2]=(byte)(bgra?color>>16:color);bytes[i*4+3]=(byte)(color>>24);}
                        long next; synchronized(AppUseAccessibilityService.this){next=++sequence;}
                        result.complete(new AppUseProjectionService.Frame(bytes,width,height,nativeWidth,nativeHeight,format,next,SystemClock.elapsedRealtimeNanos(),System.currentTimeMillis(),(System.nanoTime()-started)/1000));
                    } catch(Exception error){result.completeExceptionally(error);}
                    finally {if(scaled!=null&&scaled!=pixels)scaled.recycle();if(pixels!=null)pixels.recycle();if(hardware!=null)hardware.recycle();shot.getHardwareBuffer().close();}
                }
            });
            try { return result.get(2,TimeUnit.SECONDS); }
            finally { result.cancel(false); }
        }
    }
}
