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

    private native void sinomoLifecycleChanged(boolean resumed);

    private native void sinomoWindowInsetsChanged(int left, int top, int right, int bottom, float density);

    private native void sinomoOperatingConditionsChanged();
}
