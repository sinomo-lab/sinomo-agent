package net.sinomo.probe;
import java.io.*;
import java.nio.*;
import net.sinomo.app.runtime.SinomoVideoEncoder;
import net.sinomo.app.runtime.SinomoVideoDecoder;

/** Standalone app_process fixture: no capture, networking, permissions or app data. */
public final class DecoderProbe {
    private static native String run(Object callback, String path, boolean hardware, boolean rgbEveryFrame);
    private boolean unsupported;
    public SinomoVideoDecoder openVideoDecoder(int width,int height) throws Exception {
        if (unsupported) throw new IOException("Test hardware unavailable");
        return new SinomoVideoDecoder(width,height);
    }
    public static void main(String[] args) throws Exception {
        System.load(args[0]);
        File file = new File(args[1],"frames.bin");
        if (!file.exists()) {
            ByteBuffer pixels=ByteBuffer.allocateDirect(640*360*3);
            try (SinomoVideoEncoder encoder=new SinomoVideoEncoder(640,360,10);DataOutputStream out=new DataOutputStream(new FileOutputStream(file))) {
                for(int frame=0;frame<105;frame++) {
                    pixels.clear();
                    for(int y=0;y<360;y++)for(int x=0;x<640;x++)pixels.put((byte)(x<320?200:40)).put((byte)(y<180?60:200)).put((byte)(40+frame%100));
                    pixels.flip();byte[] access=encoder.encode(pixels,640,360,0);out.writeInt(access.length);out.write(access);
                }
            }
        }
        DecoderProbe callback=new DecoderProbe();
        String software=run(callback,file.getAbsolutePath(),false,true);
        callback.unsupported=true;
        String fallback=run(callback,file.getAbsolutePath(),true,true);
        callback.unsupported=false;
        String hardware=run(callback,file.getAbsolutePath(),true,true);
        String nativeFrames=run(callback,file.getAbsolutePath(),true,false);
        System.out.println("NATIVE "+nativeFrames);
        if(nativeFrames.contains("\"error\"")||!nativeFrames.contains("\"rgb_every_frame\":false"))throw new IOException("Native consumer verification failed");
        System.out.println("SOFTWARE "+software);
        System.out.println("FALLBACK "+fallback);
        System.out.println("HARDWARE "+hardware);
        if(fallback.contains("\"error\"")||!fallback.contains("openh264")||software.contains("\"error\"")||hardware.contains("\"error\"")||!hardware.contains("android-mediacodec"))throw new IOException("Decoder verification failed");
    }
}
