package net.sinomo.appuse.inspect;

import android.app.Instrumentation;
import android.app.UiAutomation;
import android.graphics.Rect;
import android.os.Bundle;
import android.view.accessibility.AccessibilityNodeInfo;
import org.json.JSONArray;
import org.json.JSONObject;

/** Read-only UI inspection which does not suppress the service under test. */
public final class Inspect extends Instrumentation {
    @Override public void onCreate(Bundle arguments) { super.onCreate(arguments); start(); }
    @Override public void onStart() {
        Bundle result = new Bundle();
        try {
            UiAutomation automation = getUiAutomation(UiAutomation.FLAG_DONT_SUPPRESS_ACCESSIBILITY_SERVICES);
            AccessibilityNodeInfo root = automation.getRootInActiveWindow();
            JSONArray nodes = new JSONArray();
            if (root != null) { try { visit(root, nodes, 0); } finally { root.recycle(); } }
            result.putString("nodes", nodes.toString());
            finish(0, result);
        } catch (Exception error) { result.putString("error", error.toString()); finish(1, result); }
    }
    private void visit(AccessibilityNodeInfo node, JSONArray nodes, int depth) throws Exception {
        if (depth > 40 || nodes.length() >= 2048) return;
        Rect bounds = new Rect(); node.getBoundsInScreen(bounds);
        nodes.put(new JSONObject().put("text", node.getText() == null ? "" : node.getText().toString())
                .put("description", node.getContentDescription() == null ? "" : node.getContentDescription().toString())
                .put("checked", node.isChecked()).put("clickable", node.isClickable())
                .put("bounds", new JSONArray(new int[]{bounds.left,bounds.top,bounds.right,bounds.bottom})));
        for (int i = 0; i < node.getChildCount(); i++) {
            AccessibilityNodeInfo child = node.getChild(i);
            if (child != null) { try { visit(child, nodes, depth + 1); } finally { child.recycle(); } }
        }
    }
}
