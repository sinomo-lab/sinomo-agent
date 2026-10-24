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
    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        LinearLayout view = new LinearLayout(this);
        view.setOrientation(LinearLayout.VERTICAL);
        view.setPadding(24, 24, 24, 24);
        TextView description = new TextView(this);
        description.setText("App Use can view and control your shared display. First open Sinomo and enable Remote Management in its local Settings. Enable accessibility here, share your screen, then grant access to this share below. Stop sharing or disable accessibility to revoke access.");
        view.addView(description);
        Button accessibility = new Button(this);
        accessibility.setText("Open accessibility settings");
        accessibility.setOnClickListener(v -> startActivity(new Intent(Settings.ACTION_ACCESSIBILITY_SETTINGS)));
        view.addView(accessibility);
        Button share = new Button(this);
        share.setText("Share screen for App Use");
        share.setOnClickListener(v -> {
            MediaProjectionManager manager = getSystemService(MediaProjectionManager.class);
            // Raw input coordinates refer to the default physical display.
            // Android 14's app-only capture would have a different coordinate space.
            Intent consent = Build.VERSION.SDK_INT >= 34
                    ? manager.createScreenCaptureIntent(MediaProjectionConfig.createConfigForDefaultDisplay())
                    : manager.createScreenCaptureIntent();
            startActivityForResult(consent, 1);
        });
        view.addView(share);
        CheckBox input = new CheckBox(this);
        input.setText("Allow touch and text input for this share");
        view.addView(input);
        TextView status = new TextView(this);
        Button grant = new Button(this);
        grant.setText("Allow capture and selected input for this share");
        grant.setOnClickListener(v -> {
            synchronized (AppUseProjectionService.LOCK) {
                if (AppUseProjectionService.target == null) { status.setText("Start screen sharing first."); return; }
                try {
                    boolean allowed = grantDisplay("display:" + AppUseProjectionService.target, input.isChecked());
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
        setContentView(view);
    }
    @Override protected void onActivityResult(int request, int result, Intent data) {
        super.onActivityResult(request, result, data);
        if (request == 1 && result == RESULT_OK && data != null) {
            startForegroundService(new Intent(this, AppUseProjectionService.class)
                    .putExtra("result", result).putExtra("consent", data));
        }
    }
    private static native boolean grantDisplay(String resource, boolean input);
}
