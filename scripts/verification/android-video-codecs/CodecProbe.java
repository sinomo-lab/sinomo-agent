package net.sinomo.probe;

import android.graphics.*;
import android.media.*;
import android.opengl.*;
import android.os.*;
import android.view.Surface;
import java.io.*;
import java.nio.*;
import java.util.*;
import org.json.*;

/** Model-free MediaCodec surface/decoder experiment; no camera or screen grants. */
public final class CodecProbe {
    static final int WIDTH = 304, HEIGHT = 640, FPS = 10;
    static File root;
    static int[] reference;
    static int failures;
    static void emit(JSONObject value) {
        System.out.println("CODEC_PROBE " + value); System.out.flush();
        if(root!=null)try(FileOutputStream out=new FileOutputStream(new File(root,"results.jsonl"),true)) {
            out.write((value.toString()+"\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));
        } catch(IOException error) { throw new IllegalStateException(error); }
    }
    static JSONObject row(String kind) throws Exception { return new JSONObject().put("kind", kind); }
    static void write(File file, byte[] bytes) throws Exception {
        try (FileOutputStream out = new FileOutputStream(file)) { out.write(bytes); }
    }
    static byte[] pixels(Bitmap bitmap, boolean bgra) {
        int[] colors = new int[bitmap.getWidth() * bitmap.getHeight()];
        bitmap.getPixels(colors, 0, bitmap.getWidth(), 0, 0, bitmap.getWidth(), bitmap.getHeight());
        byte[] out = new byte[colors.length * 4];
        for (int i=0; i<colors.length; i++) {
            int c=colors[i]; out[4*i]=(byte)(bgra ? c : c >> 16); out[4*i+1]=(byte)(c >> 8);
            out[4*i+2]=(byte)(bgra ? c >> 16 : c); out[4*i+3]=(byte)255;
        }
        return out;
    }
    static Bitmap fixtures() throws Exception {
        Bitmap src=Bitmap.createBitmap(1080,2280,Bitmap.Config.ARGB_8888);
        Canvas c=new Canvas(src); c.drawColor(Color.WHITE); Paint p=new Paint(Paint.ANTI_ALIAS_FLAG);
        p.setColor(Color.rgb(15,120,210)); c.drawRect(24,430,1056,1210,p);
        p.setColor(Color.YELLOW); c.drawRect(140,520,460,770,p);
        p.setColor(Color.BLACK); p.setTextSize(66); c.drawText("Sinomo codec comparison",32,100,p);
        p.setTextSize(42); c.drawText("Text, edges, gradients and motion",32,190,p);
        c.drawText("0123456789 ABC abc",32,280,p);
        for(int i=0;i<256;i++) { p.setColor(Color.rgb(i,255-i,128)); c.drawRect(24+i*4,1300,28+i*4,1600,p); }
        p.setStrokeWidth(3); p.setColor(Color.BLACK);
        for(int y=1700;y<2100;y+=12)c.drawLine(30,y,1040,y,p);
        try(FileOutputStream out=new FileOutputStream(new File(root,"source.png"))) { src.compress(Bitmap.CompressFormat.PNG,100,out); }
        Bitmap small=Bitmap.createScaledBitmap(src,WIDTH,HEIGHT,true);
        write(new File(root,"source.bgra"),pixels(src,true));
        write(new File(root,"reference.bgra"),pixels(small,true));
        write(new File(root,"reference.rgba"),pixels(small,false));
        try(FileOutputStream out=new FileOutputStream(new File(root,"reference.png"))) { small.compress(Bitmap.CompressFormat.PNG,100,out); }
        reference=new int[WIDTH*HEIGHT];small.getPixels(reference,0,WIDTH,0,0,WIDTH,HEIGHT);
        src.recycle();return small;
    }
    static MediaCodecInfo codec(String mime, boolean encoder) {
        for(MediaCodecInfo info:new MediaCodecList(MediaCodecList.REGULAR_CODECS).getCodecInfos()) {
            if(info.isEncoder()!=encoder || !info.isHardwareAccelerated() || info.isAlias())continue;
            for(String type:info.getSupportedTypes())if(type.equalsIgnoreCase(mime))return info;
        }
        return null;
    }
    static void inventory() throws Exception {
        for(MediaCodecInfo info:new MediaCodecList(MediaCodecList.ALL_CODECS).getCodecInfos()) {
            for(String mime:info.getSupportedTypes()) {
                if(!mime.equals("video/avc") && !mime.equals("video/hevc"))continue;
                MediaCodecInfo.CodecCapabilities caps=info.getCapabilitiesForType(mime);
                emit(row("inventory").put("name",info.getName()).put("mime",mime).put("encoder",info.isEncoder())
                    .put("hardware",info.isHardwareAccelerated()).put("software",info.isSoftwareOnly()).put("alias",info.isAlias())
                    .put("width_alignment",caps.getVideoCapabilities().getWidthAlignment())
                    .put("height_alignment",caps.getVideoCapabilities().getHeightAlignment())
                    .put("size_rate_supported",caps.getVideoCapabilities().areSizeAndRateSupported(WIDTH,HEIGHT,FPS))
                    .put("color_formats",new JSONArray(caps.colorFormats)));
            }
        }
    }
    static final class Renderer implements AutoCloseable {
        EGLDisplay display=EGL14.EGL_NO_DISPLAY; EGLContext context=EGL14.EGL_NO_CONTEXT; EGLSurface window=EGL14.EGL_NO_SURFACE;
        int texture, program; final FloatBuffer vertices;
        Renderer(Surface surface, Bitmap source) throws Exception {
            vertices=ByteBuffer.allocateDirect(16*4).order(ByteOrder.nativeOrder()).asFloatBuffer();
            vertices.put(new float[]{-1,-1,0,1, 1,-1,1,1, -1,1,0,0, 1,1,1,0}).position(0);
            try {
            display=EGL14.eglGetDisplay(EGL14.EGL_DEFAULT_DISPLAY);int[] versions=new int[2];
            check(EGL14.eglInitialize(display,versions,0,versions,1),"eglInitialize");
            EGLConfig[] configs=new EGLConfig[1];int[] count=new int[1];
            int[] attributes={EGL14.EGL_RED_SIZE,8,EGL14.EGL_GREEN_SIZE,8,EGL14.EGL_BLUE_SIZE,8,EGL14.EGL_ALPHA_SIZE,8,
                EGL14.EGL_RENDERABLE_TYPE,EGL14.EGL_OPENGL_ES2_BIT,0x3142,1,EGL14.EGL_NONE};
            check(EGL14.eglChooseConfig(display,attributes,0,configs,0,1,count,0)&&count[0]>0,"eglChooseConfig");
            context=EGL14.eglCreateContext(display,configs[0],EGL14.EGL_NO_CONTEXT,new int[]{EGL14.EGL_CONTEXT_CLIENT_VERSION,2,EGL14.EGL_NONE},0);
            window=EGL14.eglCreateWindowSurface(display,configs[0],surface,new int[]{EGL14.EGL_NONE},0);
            check(EGL14.eglMakeCurrent(display,window,window,context),"eglMakeCurrent");
            int vs=shader(GLES20.GL_VERTEX_SHADER,"attribute vec2 p;attribute vec2 t;varying vec2 uv;void main(){gl_Position=vec4(p,0.,1.);uv=t;}");
            int fs=shader(GLES20.GL_FRAGMENT_SHADER,"precision mediump float;varying vec2 uv;uniform sampler2D tex;void main(){gl_FragColor=texture2D(tex,uv);}");
            program=GLES20.glCreateProgram();GLES20.glAttachShader(program,vs);GLES20.glAttachShader(program,fs);GLES20.glLinkProgram(program);
            int[] ok=new int[1];GLES20.glGetProgramiv(program,GLES20.GL_LINK_STATUS,ok,0);check(ok[0]!=0,"shader link");
            GLES20.glDeleteShader(vs);GLES20.glDeleteShader(fs);
            int[] ids=new int[1];GLES20.glGenTextures(1,ids,0);texture=ids[0];GLES20.glBindTexture(GLES20.GL_TEXTURE_2D,texture);
            GLES20.glTexParameteri(GLES20.GL_TEXTURE_2D,GLES20.GL_TEXTURE_MIN_FILTER,GLES20.GL_LINEAR);
            GLES20.glTexParameteri(GLES20.GL_TEXTURE_2D,GLES20.GL_TEXTURE_MAG_FILTER,GLES20.GL_LINEAR);
            byte[] rgba=pixels(source,false);ByteBuffer input=ByteBuffer.allocateDirect(rgba.length);input.put(rgba).position(0);
            GLES20.glTexImage2D(GLES20.GL_TEXTURE_2D,0,GLES20.GL_RGBA,source.getWidth(),source.getHeight(),0,GLES20.GL_RGBA,GLES20.GL_UNSIGNED_BYTE,input);
            check(GLES20.glGetError()==GLES20.GL_NO_ERROR,"texture upload");
            } catch (Exception error) { close(); throw error; }
        }
        static int shader(int type,String source) throws Exception {
            int s=GLES20.glCreateShader(type);GLES20.glShaderSource(s,source);GLES20.glCompileShader(s);int[] ok=new int[1];GLES20.glGetShaderiv(s,GLES20.GL_COMPILE_STATUS,ok,0);
            if(ok[0]==0)throw new IOException(GLES20.glGetShaderInfoLog(s));return s;
        }
        void draw(int sequence,long ptsUs,boolean moving) throws Exception {
            GLES20.glViewport(0,0,WIDTH,HEIGHT);GLES20.glDisable(GLES20.GL_SCISSOR_TEST);GLES20.glUseProgram(program);
            int pos=GLES20.glGetAttribLocation(program,"p"),tex=GLES20.glGetAttribLocation(program,"t");
            vertices.position(0);GLES20.glVertexAttribPointer(pos,2,GLES20.GL_FLOAT,false,16,vertices);GLES20.glEnableVertexAttribArray(pos);
            vertices.position(2);GLES20.glVertexAttribPointer(tex,2,GLES20.GL_FLOAT,false,16,vertices);GLES20.glEnableVertexAttribArray(tex);
            GLES20.glDrawArrays(GLES20.GL_TRIANGLE_STRIP,0,4);
            if(moving) { GLES20.glEnable(GLES20.GL_SCISSOR_TEST);GLES20.glScissor((sequence*7)%(WIDTH-8),0,8,HEIGHT);GLES20.glClearColor(1,0,0,1);GLES20.glClear(GLES20.GL_COLOR_BUFFER_BIT);GLES20.glDisable(GLES20.GL_SCISSOR_TEST); }
            check(EGLExt.eglPresentationTimeANDROID(display,window,ptsUs*1000),"presentation timestamp");
            check(EGL14.eglSwapBuffers(display,window),"eglSwapBuffers");
        }
        public void close() {
            if(display!=EGL14.EGL_NO_DISPLAY) {
                if(context!=EGL14.EGL_NO_CONTEXT) { GLES20.glDeleteProgram(program);GLES20.glDeleteTextures(1,new int[]{texture},0); }
                EGL14.eglMakeCurrent(display,EGL14.EGL_NO_SURFACE,EGL14.EGL_NO_SURFACE,EGL14.EGL_NO_CONTEXT);
                if(window!=EGL14.EGL_NO_SURFACE)EGL14.eglDestroySurface(display,window);
                if(context!=EGL14.EGL_NO_CONTEXT)EGL14.eglDestroyContext(display,context);
                EGL14.eglReleaseThread();EGL14.eglTerminate(display);
            }
        }
    }
    static void check(boolean value,String message) throws IOException { if(!value)throw new IOException(message); }
    static double lumaPsnr(Image image,int sequence,boolean moving,MediaFormat format) throws Exception {
        Rect crop=image.getCropRect();check(crop.width()==WIDTH&&crop.height()==HEIGHT,"decoded geometry");
        Image.Plane plane=image.getPlanes()[0];ByteBuffer y=plane.getBuffer();int base=y.position();double squared=0;
        int standard=format.containsKey(MediaFormat.KEY_COLOR_STANDARD)?format.getInteger(MediaFormat.KEY_COLOR_STANDARD):MediaFormat.COLOR_STANDARD_BT709;
        int range=format.containsKey(MediaFormat.KEY_COLOR_RANGE)?format.getInteger(MediaFormat.KEY_COLOR_RANGE):MediaFormat.COLOR_RANGE_LIMITED;
        double cr=standard==MediaFormat.COLOR_STANDARD_BT709?.2126:.299, cb=standard==MediaFormat.COLOR_STANDARD_BT709?.0722:.114;
        int marker=(sequence*7)%(WIDTH-8);
        for(int row=0;row<HEIGHT;row++)for(int col=0;col<WIDTH;col++) {
            int color=moving&&col>=marker&&col<marker+8?Color.RED:reference[row*WIDTH+col];
            double expected=cr*Color.red(color)+(1-cr-cb)*Color.green(color)+cb*Color.blue(color);
            if(range!=MediaFormat.COLOR_RANGE_FULL)expected=16+expected*219/255;
            double actual=y.get(base+(row+crop.top)*plane.getRowStride()+(col+crop.left)*plane.getPixelStride())&255;
            double delta=actual-expected;squared+=delta*delta;
        }
        return squared==0?99:10*Math.log10(255.*255*WIDTH*HEIGHT/squared);
    }
    static void run(String mime,int bitrate,boolean allKey,int frames,int repetition,Bitmap source) throws Exception {
        run(mime,bitrate,allKey,frames,repetition,source,100);
    }
    static void run(String mime,int bitrate,boolean allKey,int frames,int repetition,Bitmap source,int spacingMs) throws Exception {
        MediaCodecInfo encoderInfo=codec(mime,true),decoderInfo=codec(mime,false);
        if(encoderInfo==null||decoderInfo==null) { emit(row("unsupported").put("mime",mime).put("encoder",encoderInfo!=null).put("decoder",decoderInfo!=null));return; }
        String lane=(mime.equals("video/avc")?"h264":"h265")+"-"+bitrate+"-"+(allKey?"independent":"stream")+"-"+repetition
            +(source.getWidth()==WIDTH?"":"-gpu-scale")+(spacingMs==100?"":"-sparse");
        MediaCodec encoder=null,decoder=null;Surface input=null;Renderer renderer=null;
        long caseStart=System.nanoTime();
        try(FileOutputStream stream=new FileOutputStream(new File(root,lane+".annexb"))) {
            encoder=MediaCodec.createByCodecName(encoderInfo.getName());
            MediaFormat format=MediaFormat.createVideoFormat(mime,WIDTH,HEIGHT);
            format.setInteger(MediaFormat.KEY_COLOR_FORMAT,MediaCodecInfo.CodecCapabilities.COLOR_FormatSurface);
            format.setInteger(MediaFormat.KEY_BIT_RATE,bitrate);format.setInteger(MediaFormat.KEY_FRAME_RATE,FPS);
            format.setInteger(MediaFormat.KEY_I_FRAME_INTERVAL,allKey?0:1);format.setInteger(MediaFormat.KEY_MAX_B_FRAMES,0);
            format.setInteger(MediaFormat.KEY_LATENCY,0);format.setInteger(MediaFormat.KEY_PRIORITY,0);
            format.setInteger(MediaFormat.KEY_COLOR_STANDARD,MediaFormat.COLOR_STANDARD_BT709);
            format.setInteger(MediaFormat.KEY_COLOR_RANGE,MediaFormat.COLOR_RANGE_LIMITED);
            format.setInteger(MediaFormat.KEY_COLOR_TRANSFER,MediaFormat.COLOR_TRANSFER_SDR_VIDEO);
            MediaCodecInfo.EncoderCapabilities ec=encoderInfo.getCapabilitiesForType(mime).getEncoderCapabilities();
            if(ec.isBitrateModeSupported(MediaCodecInfo.EncoderCapabilities.BITRATE_MODE_CBR))format.setInteger(MediaFormat.KEY_BITRATE_MODE,MediaCodecInfo.EncoderCapabilities.BITRATE_MODE_CBR);
            encoder.configure(format,null,null,MediaCodec.CONFIGURE_FLAG_ENCODE);input=encoder.createInputSurface();encoder.start();
            renderer=new Renderer(input,source);long setupEnd=System.nanoTime();
            MediaFormat encodedFormat=null,decodedFormat=null;int configBytes=0;boolean wroteConfig=false;
            long firstSubmit=0, previousSubmit=0;
            for(int frame=0;frame<frames;frame++) {
                if(frame>0) {
                    long delay=firstSubmit+(long)frame*spacingMs*1_000_000-System.nanoTime();
                    if(delay>0)java.util.concurrent.locks.LockSupport.parkNanos(delay);
                }
                if(allKey) { Bundle request=new Bundle();request.putInt(MediaCodec.PARAMETER_KEY_REQUEST_SYNC_FRAME,0);encoder.setParameters(request); }
                long ptsUs=(long)frame*spacingMs*1000;
                long submit=System.nanoTime();if(frame==0)firstSubmit=submit;
                renderer.draw(frame,ptsUs,!allKey);long queued=System.nanoTime();
                MediaCodec.BufferInfo info=new MediaCodec.BufferInfo();byte[] access=null;long encodedAt=0;boolean key=false;
                long deadline=System.nanoTime()+1_000_000_000L;
                while(access==null&&System.nanoTime()<deadline) {
                    int index=encoder.dequeueOutputBuffer(info,1000);
                    if(index==MediaCodec.INFO_OUTPUT_FORMAT_CHANGED)encodedFormat=encoder.getOutputFormat();
                    else if(index>=0) {
                        if((info.flags&MediaCodec.BUFFER_FLAG_CODEC_CONFIG)==0&&info.size>0) {
                            check(info.presentationTimeUs==ptsUs,"encoder reordered or delayed timestamps");
                            check((info.flags&MediaCodec.BUFFER_FLAG_PARTIAL_FRAME)==0,"partial access unit requires assembly");
                            encodedAt=System.nanoTime();key=(info.flags&MediaCodec.BUFFER_FLAG_KEY_FRAME)!=0;
                            ByteBuffer data=encoder.getOutputBuffer(index).duplicate();data.position(info.offset).limit(info.offset+info.size);access=new byte[info.size];data.get(access);
                        }
                        encoder.releaseOutputBuffer(index,false);
                    }
                }
                check(access!=null,"no encoded frame within one second with one frame in flight");
                check(!allKey||key,"encoder did not provide an independent keyframe");
                if(!wroteConfig) {
                    check(encodedFormat!=null,"missing codec configuration");
                    for(int i=0;encodedFormat.containsKey("csd-"+i);i++) {
                        ByteBuffer csd=encodedFormat.getByteBuffer("csd-"+i).duplicate();byte[] data=new byte[csd.remaining()];csd.get(data);configBytes+=data.length;stream.write(data);
                    }
                    wroteConfig=true;
                    decoder=MediaCodec.createByCodecName(decoderInfo.getName());
                    encodedFormat.setInteger(MediaFormat.KEY_COLOR_FORMAT,MediaCodecInfo.CodecCapabilities.COLOR_FormatYUV420Flexible);
                    if(decoderInfo.getCapabilitiesForType(mime).isFeatureSupported(MediaCodecInfo.CodecCapabilities.FEATURE_LowLatency))encodedFormat.setInteger(MediaFormat.KEY_LOW_LATENCY,1);
                    decoder.configure(encodedFormat,null,null,0);decoder.start();
                    emit(row("configuration").put("lane",lane).put("encoder",encoderInfo.getName()).put("decoder",decoderInfo.getName())
                        .put("hardware",true).put("source_interval_ms",spacingMs).put("input_width",source.getWidth()).put("input_height",source.getHeight()).put("requested",format.toString()).put("encoder_output",encoder.getOutputFormat().toString())
                        .put("setup_ms",(setupEnd-caseStart)/1e6).put("codec_config_bytes",configBytes));
                }
                long decodeStart=System.nanoTime();int in=decoder.dequeueInputBuffer(1_000_000);check(in>=0,"decoder input timeout");
                ByteBuffer data=decoder.getInputBuffer(in);check(data.capacity()>=access.length,"decoder input capacity");data.clear();data.put(access);
                decoder.queueInputBuffer(in,0,access.length,ptsUs,key?MediaCodec.BUFFER_FLAG_KEY_FRAME:0);
                int decoded=-1;deadline=System.nanoTime()+1_000_000_000L;
                while(decoded<0&&System.nanoTime()<deadline) {
                    int index=decoder.dequeueOutputBuffer(info,1000);
                    if(index==MediaCodec.INFO_OUTPUT_FORMAT_CHANGED)decodedFormat=decoder.getOutputFormat();
                    else if(index>=0)decoded=index;
                }
                check(decoded>=0,"decoder output timeout");long ready=System.nanoTime();
                check(info.presentationTimeUs==ptsUs,"decoder timestamp mismatch");
                stream.write(access);
                double psnr;
                try(Image image=decoder.getOutputImage(decoded)) {
                    check(image!=null,"hardware decoder did not expose a validation image");
                    psnr=lumaPsnr(image,frame,!allKey,decodedFormat==null?decoder.getOutputFormat():decodedFormat);
                } finally { decoder.releaseOutputBuffer(decoded,false); }
                emit(row("sample").put("lane",lane).put("frame",frame).put("warmup",frame<5).put("keyframe",key)
                    .put("bytes",access.length).put("codec_config_bytes",configBytes)
                    .put("source_gap_ms",frame==0?JSONObject.NULL:(submit-previousSubmit)/1e6).put("submit_ns",submit).put("ready_ns",ready).put("submit_ms",(queued-submit)/1e6)
                    .put("encode_ms",(encodedAt-submit)/1e6).put("decode_ms",(ready-decodeStart)/1e6)
                    .put("encode_decode_ms",(ready-submit)/1e6).put("cold_first_frame_ms",frame==0?(ready-caseStart)/1e6:JSONObject.NULL)
                    .put("luma_psnr_db",psnr).put("decoded_format",frame==0?decoder.getOutputFormat().toString():JSONObject.NULL));
                previousSubmit=submit;
            }
            emit(row("complete").put("lane",lane).put("frames",frames));
        } catch(Exception error) { failures++; emit(row("failure").put("lane",lane).put("error",error.toString())); }
        finally {
            if(renderer!=null)renderer.close();if(input!=null)input.release();
            if(decoder!=null) { try {decoder.stop();}catch(Exception ignored){} decoder.release(); }
            if(encoder!=null) { try {encoder.stop();}catch(Exception ignored){} encoder.release(); }
        }
    }
    public static void main(String[] args) throws Exception {
        if(args.length<2)throw new IllegalArgumentException("CodecProbe inventory|fixtures|run OUTPUT_DIR [frames]");
        root=new File(args[1]);if(!root.exists()&&!root.mkdirs())throw new IOException("output directory");
        write(new File(root,"results.jsonl"),new byte[0]);failures=0;
        if(args[0].equals("inventory")) {inventory();return;}
        Bitmap bitmap=fixtures();
        if(args[0].equals("fixtures"))return;
        int frames=args.length>2?Integer.parseInt(args[2]):35;
        try {
            inventory();
            for(String mime:new String[]{"video/avc","video/hevc"}) {
                if(args[0].equals("sparse")) { run(mime,1_000_000,true,frames,0,bitmap,1000); continue; }
                for(int bitrate:new int[]{500_000,1_000_000,2_000_000}) {
                    run(mime,bitrate,true,frames,0,bitmap);
                    run(mime,bitrate,false,frames,0,bitmap);
                }
                for(int repeat=1;repeat<=3;repeat++)run(mime,1_000_000,true,1,repeat,bitmap);
                Bitmap full=BitmapFactory.decodeFile(new File(root,"source.png").getAbsolutePath());
                try { run(mime,1_000_000,true,frames,0,full); run(mime,1_000_000,false,frames,0,full); }
                finally {full.recycle();}
            }
        } finally {bitmap.recycle();}
        emit(row("summary").put("failures",failures));
        if(failures>0)throw new IOException("codec lanes failed: " + failures);
    }
}
