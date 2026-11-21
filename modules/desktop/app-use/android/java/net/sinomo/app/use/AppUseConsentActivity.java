package net.sinomo.app.use;

import android.app.ActionBar;
import android.content.Intent;
import android.media.projection.MediaProjectionConfig;
import android.media.projection.MediaProjectionManager;
import android.os.Build;
import android.os.Bundle;
import android.preference.ListPreference;
import android.preference.Preference;
import android.preference.PreferenceActivity;
import android.preference.SwitchPreference;
import android.provider.Settings;
import android.view.MenuItem;
import android.widget.Toast;

/** Private detail page opened from Sinomo's local Host access settings. */
public final class AppUseConsentActivity extends PreferenceActivity {
    private static final String ACCESSIBILITY = "display:" + AppUseAccessibilityService.TARGET;
    // Flags returned by the local permission store through accessState().
    private static final int REMOTE_MANAGEMENT = 1, CAPTURE = 2, INPUT = 4, STOP = 8;
    private static volatile AppUseConsentActivity visible;
    private static volatile boolean projectionConsentPending;
    private boolean requestingProjection;
    private Preference accessibilityService;
    private ListPreference persistentAccess;
    private SwitchPreference sharing;
    private SwitchPreference shareAccess;
    private String displayedShare;

    static boolean protectsLocalConsent() {
        return visible != null || projectionConsentPending;
    }

    // Android delivers these service lifecycle callbacks on the main thread.
    static void serviceStateChanged() {
        AppUseConsentActivity page = visible;
        if (page != null) page.refresh();
    }

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        requestingProjection = state != null && state.getBoolean("requesting_projection");
        if (requestingProjection) projectionConsentPending = true;
        ActionBar bar = getActionBar();
        if (bar != null) bar.setDisplayHomeAsUpEnabled(true);
        addPreferencesFromResource(getResources().getIdentifier("app_use_settings", "xml", getPackageName()));
        accessibilityService = findPreference("accessibility_service");
        persistentAccess = (ListPreference) findPreference("persistent_access");
        sharing = (SwitchPreference) findPreference("screen_sharing");
        shareAccess = (SwitchPreference) findPreference("share_access");
        accessibilityService.setOnPreferenceClickListener(pref -> {
            startActivity(new Intent(Settings.ACTION_ACCESSIBILITY_SETTINGS));
            return true;
        });
        persistentAccess.setOnPreferenceChangeListener((pref, value) -> {
            boolean revoke = "off".equals(value);
            if (!revoke && AppUseAccessibilityService.current == null) {
                notice("Turn on Sinomo App Use in Android accessibility settings first.");
            } else {
                saveAccess(ACCESSIBILITY, revoke, "control".equals(value));
            }
            refresh();
            return false; // refresh() reads the authoritative state.
        });
        sharing.setOnPreferenceChangeListener((pref, enabled) -> {
            if (Boolean.TRUE.equals(enabled)) requestProjection();
            else stopService(new Intent(this, AppUseProjectionService.class));
            return false; // The service reports when sharing starts or stops.
        });
        shareAccess.setOnPreferenceChangeListener((pref, enabled) -> {
            String target = currentShare();
            if (target != null && target.equals(displayedShare)) {
                saveAccess(target, !Boolean.TRUE.equals(enabled), false);
            }
            refresh();
            return false;
        });
    }

    private static String currentShare() {
        synchronized (AppUseProjectionService.LOCK) {
            String target = AppUseProjectionService.target;
            return target == null ? null : "display:" + target;
        }
    }

    private void refresh() {
        boolean connected = AppUseAccessibilityService.current != null;
        accessibilityService.setSummary(connected ? "On · Open Android settings" : "Off · Open Android settings");
        int flags = readAccess(ACCESSIBILITY);
        boolean remote = flags >= 0 && (flags & REMOTE_MANAGEMENT) != 0;
        boolean observe = flags >= 0 && (flags & CAPTURE) != 0;
        String level = !observe ? "off" : (flags & INPUT) != 0 ? "control" : "observe";
        persistentAccess.setValue(level);
        persistentAccess.setEnabled(observe || (connected && remote));
        String summary = String.valueOf(persistentAccess.getEntry());
        if (flags < 0) summary = "Local permission settings are unavailable.";
        else if (!remote) summary += " · Enable Remote management in Host access to allow agent access.";
        else if (!connected) summary += " · Turn on the Android accessibility service to allow agent access.";
        persistentAccess.setSummary(summary);

        displayedShare = currentShare();
        sharing.setChecked(displayedShare != null);
        sharing.setEnabled(!projectionConsentPending);
        int share = displayedShare == null ? 0 : readAccess(displayedShare);
        boolean granted = share >= 0 && (share & (CAPTURE | STOP)) == (CAPTURE | STOP);
        shareAccess.setChecked(granted);
        shareAccess.setEnabled(displayedShare != null && share >= 0 && (remote || granted));
        shareAccess.setSummary(displayedShare == null ? "Available while sharing your screen."
                : share < 0 ? "Local permission settings are unavailable."
                : !remote ? "Agent access requires Remote management in Host access." : null);
    }

    private static int readAccess(String resource) {
        try {
            return accessState(resource);
        } catch (UnsatisfiedLinkError | RuntimeException unavailable) {
            return -1;
        }
    }

    private void saveAccess(String resource, boolean revoke, boolean input) {
        try {
            if (revoke ? revokeAccess(resource) : grantDisplay(resource, input)) return;
            notice(revoke ? "Could not revoke access." : "Enable Remote management in Host access to grant access.");
        } catch (UnsatisfiedLinkError | RuntimeException unavailable) {
            notice("Local permission settings are unavailable.");
        }
    }

    private void requestProjection() {
        if (projectionConsentPending || currentShare() != null) return;
        MediaProjectionManager manager = getSystemService(MediaProjectionManager.class);
        // Coordinates refer to the default display, including on Android 14+.
        Intent consent = Build.VERSION.SDK_INT >= 34
                ? manager.createScreenCaptureIntent(MediaProjectionConfig.createConfigForDefaultDisplay())
                : manager.createScreenCaptureIntent();
        requestingProjection = projectionConsentPending = true;
        try {
            startActivityForResult(consent, 1);
        } catch (RuntimeException unavailable) {
            requestingProjection = projectionConsentPending = false;
            notice("Android could not start screen sharing.");
            refresh();
        }
    }

    @Override protected void onActivityResult(int request, int result, Intent data) {
        super.onActivityResult(request, result, data);
        if (request != 1) return;
        requestingProjection = projectionConsentPending = false;
        if (result == RESULT_OK && data != null) {
            startForegroundService(new Intent(this, AppUseProjectionService.class)
                    .putExtra("result", result).putExtra("consent", data));
        }
    }

    @Override protected void onStart() {
        super.onStart();
        visible = this;
    }
    @Override protected void onResume() {
        super.onResume();
        refresh();
    }
    @Override protected void onStop() {
        if (visible == this) visible = null;
        super.onStop();
    }
    @Override protected void onSaveInstanceState(Bundle state) {
        state.putBoolean("requesting_projection", requestingProjection);
        super.onSaveInstanceState(state);
    }
    @Override protected void onDestroy() {
        if (visible == this) visible = null;
        if (requestingProjection && !isChangingConfigurations()) projectionConsentPending = false;
        super.onDestroy();
    }
    @Override public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() != android.R.id.home) return super.onOptionsItemSelected(item);
        finish();
        return true;
    }

    private void notice(String message) {
        Toast.makeText(this, message, Toast.LENGTH_LONG).show();
    }

    private static native int accessState(String resource);
    private static native boolean grantDisplay(String resource, boolean input);
    private static native boolean revokeAccess(String resource);
}
