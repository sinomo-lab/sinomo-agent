package net.sinomo.app.runtime;

import android.app.Activity;
import android.app.Application;
import android.os.Bundle;

/**
 * Bridges authoritative Activity lifecycle transitions to the Rust mobile
 * runtime. The Application retains this callback after start(), so no Java
 * object needs to be held by Rust.
 */
public final class SinomoLifecycleCallbacks implements Application.ActivityLifecycleCallbacks {
    private final Activity activity;
    private boolean registered;

    public SinomoLifecycleCallbacks(Activity activity) {
        this.activity = activity;
    }

    public void start() {
        if (registered) {
            return;
        }
        activity.getApplication().registerActivityLifecycleCallbacks(this);
        registered = true;

        // Registration can happen after onActivityResumed on some NativeActivity
        // startup paths. Focus is sufficient evidence that this Activity is
        // currently resumed; a false value remains conservative.
        if (activity.hasWindowFocus()) {
            sinomoLifecycleChanged(true);
        }
    }

    private boolean isTarget(Activity candidate) {
        return candidate == activity;
    }

    @Override
    public void onActivityResumed(Activity candidate) {
        if (isTarget(candidate)) {
            sinomoLifecycleChanged(true);
        }
    }

    @Override
    public void onActivityPaused(Activity candidate) {
        if (isTarget(candidate)) {
            sinomoLifecycleChanged(false);
        }
    }

    @Override
    public void onActivityStopped(Activity candidate) {
        if (isTarget(candidate)) {
            sinomoLifecycleChanged(false);
        }
    }

    @Override
    public void onActivityDestroyed(Activity candidate) {
        if (!isTarget(candidate)) {
            return;
        }
        sinomoLifecycleChanged(false);
        if (registered) {
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

    private native void sinomoLifecycleChanged(boolean resumed);
}
