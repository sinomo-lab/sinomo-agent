package net.sinomo.app.use;

import android.app.Activity;
import android.content.Intent;
import android.media.projection.MediaProjectionConfig;
import android.media.projection.MediaProjectionManager;
import android.os.Build;
import android.os.Bundle;
import android.provider.Settings;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.LinearLayout;
import android.widget.TextView;

/** Local-only OS consent. No remote command starts this activity or grants access. */
public final class AppUseConsentActivity extends Activity {
    private static volatile AppUseConsentActivity visible;
    private static volatile boolean projectionConsentPending;
    private boolean requestingProjection;
    static boolean protectsLocalConsent() { return visible!=null || projectionConsentPending; }
    @Override protected void onResume() { super.onResume();visible=this; }
    @Override protected void onPause() { if(visible==this)visible=null;super.onPause(); }
    @Override protected void onSaveInstanceState(Bundle state) { state.putBoolean("requesting_projection",requestingProjection);super.onSaveInstanceState(state); }
    @Override protected void onDestroy() {
        if(visible==this)visible=null;
        if(requestingProjection && !isChangingConfigurations())projectionConsentPending=false;
        super.onDestroy();
    }

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        requestingProjection=state!=null && state.getBoolean("requesting_projection");
        if(requestingProjection)projectionConsentPending=true;
        LinearLayout view = new LinearLayout(this);
        view.setOrientation(LinearLayout.VERTICAL);
        view.setPadding(24, 24, 24, 24);
        TextView description = new TextView(this);
        description.setText("Enable Sinomo Accessibility in Android Settings for persistent App Use. Allow observation and optional control below after enabling Remote Management in Sinomo. Screen sharing is separate: it continues across app switches and without viewers until you, an authorized agent, or Android stops it.");
        view.addView(description);
        Button accessibility = new Button(this);
        accessibility.setText("Open accessibility settings");
        accessibility.setOnClickListener(v -> startActivity(new Intent(Settings.ACTION_ACCESSIBILITY_SETTINGS)));
        view.addView(accessibility);
        TextView status = new TextView(this);
        CheckBox input = new CheckBox(this);
        input.setText("Allow Accessibility touch, text and semantic actions");view.addView(input);
        Button persistent = new Button(this);persistent.setText("Allow persistent App Use observation and selected control");
        persistent.setOnClickListener(v -> {
            if(AppUseAccessibilityService.current==null){status.setText("Enable Sinomo Accessibility in Android Settings first.");return;}
            try { status.setText(grantDisplay("display:"+AppUseAccessibilityService.TARGET,input.isChecked())
                ? "App Use access saved. Screen sharing is not required." : "Enable Remote Management in Sinomo Settings first."); }
            catch(UnsatisfiedLinkError|RuntimeException error){status.setText("Open the Sinomo main app first.");}
        });view.addView(persistent);
        Button revoke = new Button(this);revoke.setText("Revoke persistent App Use access");
        revoke.setOnClickListener(v -> {
            try { status.setText(revokeAccess("display:"+AppUseAccessibilityService.TARGET)?"App Use access revoked.":"Unable to revoke access."); }
            catch(UnsatisfiedLinkError|RuntimeException error){status.setText("Open the Sinomo main app first.");}
        });view.addView(revoke);
        Button share = new Button(this);
        share.setText("Start screen sharing");
        share.setOnClickListener(v -> {
            if(projectionConsentPending){status.setText("Waiting for Android screen-sharing approval.");return;}
            synchronized(AppUseProjectionService.LOCK) {
                if(AppUseProjectionService.current!=null){status.setText("Screen sharing is already active.");return;}
            }
            MediaProjectionManager manager = getSystemService(MediaProjectionManager.class);
            // Raw input coordinates refer to the default physical display.
            // Android 14's app-only capture would have a different coordinate space.
            Intent consent = Build.VERSION.SDK_INT >= 34
                    ? manager.createScreenCaptureIntent(MediaProjectionConfig.createConfigForDefaultDisplay())
                    : manager.createScreenCaptureIntent();
            requestingProjection=true;projectionConsentPending=true;
            try { startActivityForResult(consent, 1); }
            catch(RuntimeException error) { requestingProjection=false;projectionConsentPending=false;throw error; }
        });
        view.addView(share);
        Button grant = new Button(this);
        grant.setText("Allow authorized agents to view and stop this screen share");
        grant.setOnClickListener(v -> {
            synchronized (AppUseProjectionService.LOCK) {
                if (AppUseProjectionService.target == null) { status.setText("Start screen sharing first."); return; }
                try {
                    boolean allowed = grantDisplay("display:" + AppUseProjectionService.target, false);
                    status.setText(allowed ? "Access granted to this screen share." : "Enable Remote Management in Sinomo's local Settings first.");
                } catch (UnsatisfiedLinkError | RuntimeException error) {
                    status.setText("Open the Sinomo main app first, then return here to grant access.");
                }
            }
        });
        view.addView(grant);
        view.addView(status);
        Button stop = new Button(this);
        stop.setText("Stop sharing");
        stop.setOnClickListener(v -> stopService(new Intent(this, AppUseProjectionService.class)));
        view.addView(stop);
        android.widget.ScrollView scroll=new android.widget.ScrollView(this);
        scroll.addView(view);setContentView(scroll);
    }
    @Override protected void onActivityResult(int request, int result, Intent data) {
        super.onActivityResult(request, result, data);
        if(request==1){requestingProjection=false;projectionConsentPending=false;}
        if (request == 1 && result == RESULT_OK && data != null) {
            startForegroundService(new Intent(this, AppUseProjectionService.class)
                    .putExtra("result", result).putExtra("consent", data));
        }
    }
    private static native boolean grantDisplay(String resource, boolean input);
    private static native boolean revokeAccess(String resource);
}
