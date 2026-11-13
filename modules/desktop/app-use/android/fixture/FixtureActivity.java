package net.sinomo.appuse.fixture;

import android.app.Activity;
import android.content.Intent;
import android.graphics.Canvas;
import android.graphics.Color;
import android.graphics.Paint;
import android.os.Bundle;
import android.os.Handler;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.MotionEvent;
import android.view.View;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.TextView;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import org.json.JSONObject;

/** Disposable observable app. Test input is sent through App Use, not ADB. */
public final class FixtureActivity extends Activity {
    private int clicks, downs, moves, ups, frames;
    private String text = "";
    private final Handler handler = new Handler();
    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        if (getIntent().getBooleanExtra("secure", false)) getWindow().addFlags(WindowManager.LayoutParams.FLAG_SECURE);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
        LinearLayout column = new LinearLayout(this);
        column.setOrientation(LinearLayout.VERTICAL);
        column.setPadding(24, 80, 24, 80);
        column.setBackgroundColor(Color.WHITE);
        TextView status = new TextView(this);
        status.setTextSize(24); status.setText("App Use fixture: 0 clicks");
        column.addView(status);
        Button button = new Button(this); button.setText("Count native click");
        button.setOnClickListener(v -> { clicks++; status.setText("App Use fixture: " + clicks + " clicks"); persist(); });
        column.addView(button);
        EditText editor = new EditText(this); editor.setSingleLine(true); editor.setHint("Native input test editor");
        editor.setShowSoftInputOnFocus(false);
        if(getIntent().getBooleanExtra("password",false))editor.setInputType(android.text.InputType.TYPE_CLASS_TEXT | android.text.InputType.TYPE_TEXT_VARIATION_PASSWORD);
        editor.addTextChangedListener(new TextWatcher() {
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {}
            public void onTextChanged(CharSequence s, int start, int before, int count) { text = s.toString(); persist(); }
            public void afterTextChanged(Editable e) {}
        });
        if(getIntent().getBooleanExtra("password",false))editor.setText("fixture-password");
        column.addView(editor);
        View surface = new View(this) {
            final Paint paint = new Paint();
            @Override protected void onDraw(Canvas canvas) {
                canvas.drawColor(Color.rgb(15, 120, 210));
                paint.setColor(Color.YELLOW);
                canvas.drawRect((frames % 8) * 60, 25, (frames % 8) * 60 + 100, 125, paint);
                paint.setColor(Color.BLACK); paint.setTextSize(36);
                canvas.drawText("Native frame " + frames, 30, 200, paint);
            }
            @Override public boolean onTouchEvent(MotionEvent event) {
                switch (event.getActionMasked()) {
                    case MotionEvent.ACTION_DOWN: downs++; break;
                    case MotionEvent.ACTION_MOVE: moves++; break;
                    case MotionEvent.ACTION_UP: ups++; break;
                }
                persist(); return true;
            }
        };
        surface.setContentDescription("Native gesture surface");
        column.addView(surface, new LinearLayout.LayoutParams(-1, 500));
        setContentView(column);
        handler.post(new Runnable() {
            public void run() { frames++; surface.invalidate(); handler.postDelayed(this, 200); }
        });
        persist();
    }
    private void persist() {
        try (FileOutputStream output = openFileOutput("state.json", MODE_PRIVATE)) {
            output.write(new JSONObject().put("clicks", clicks).put("text", text)
                    .put("downs", downs).put("moves", moves).put("ups", ups)
                    .put("secure", (getWindow().getAttributes().flags & WindowManager.LayoutParams.FLAG_SECURE) != 0)
                    .toString().getBytes(StandardCharsets.UTF_8));
        } catch (Exception error) { throw new RuntimeException(error); }
    }
    @Override protected void onNewIntent(Intent intent) {
        super.onNewIntent(intent); setIntent(intent);
        if (intent.getBooleanExtra("secure", false)) getWindow().addFlags(WindowManager.LayoutParams.FLAG_SECURE);
        else getWindow().clearFlags(WindowManager.LayoutParams.FLAG_SECURE);
        persist();
    }
    @Override protected void onDestroy() { handler.removeCallbacksAndMessages(null); super.onDestroy(); }
}
