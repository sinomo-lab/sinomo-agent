package net.sinomo.probe;

import android.app.Activity;
import android.os.Bundle;
import android.view.WindowManager;
import android.widget.TextView;
import java.io.File;

/** Runs the opt-in codec fixture in a normal Android graphics application. */
public final class CodecActivity extends Activity {
    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON);
        TextView status=new TextView(this);status.setTextSize(22);status.setPadding(24,80,24,24);
        status.setText("Running Sinomo hardware codec comparison…");setContentView(status);
        String mode=getIntent().getStringExtra("mode");
        final String operation=mode==null?"run":mode;
        final int frames=getIntent().getIntExtra("frames",35);
        new Thread(() -> {
            try {
                File directory=new File(getFilesDir(),"comparison");
                if(operation.equals("images") || operation.equals("images-baseline") || operation.equals("images-full")) {
                    CodecProbe.main(new String[]{"fixtures",directory.getAbsolutePath(),"0"});
                    String library=operation.equals("images-baseline")?"libimagebaseline.so":"libimagebench.so";
                    ProcessBuilder builder=new ProcessBuilder(new File(getApplicationInfo().nativeLibraryDir,library).getAbsolutePath(),
                            operation.equals("images-full")?"compare_full_source_image_pipeline":"compare_fixture_image_codecs","--ignored","--nocapture");
                    builder.environment().put("SINOMO_MEDIA_CODEC_FIXTURE_DIR",directory.getAbsolutePath());
                    builder.redirectErrorStream(true);
                    builder.redirectOutput(new File(directory,operation+".txt"));
                    int exit=builder.start().waitFor();
                    CodecProbe.emit(CodecProbe.row("image-complete").put("exit_code",exit).put("mode",operation));
                    if(exit!=0)throw new IllegalStateException("Native image probe exited " + exit);
                } else {
                    CodecProbe.main(new String[]{operation,directory.getAbsolutePath(),Integer.toString(frames)});
                }
                runOnUiThread(() -> status.setText("Codec comparison complete. Results are in the fixture's private files."));
            } catch(Exception error) {
                try { CodecProbe.emit(CodecProbe.row("fatal").put("error",error.toString())); } catch(Exception ignored) {}
                runOnUiThread(() -> status.setText("Codec comparison failed: " + error));
            }
        },"sinomo-codec-probe").start();
    }
}
