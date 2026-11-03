package net.sinomo.app.runtime;

import android.media.*;
import android.opengl.*;
import android.os.*;
import android.view.Surface;
import java.io.*;
import java.nio.*;

/** Stream-owned hardware encoder. No capture, permission or transport authority. */
public final class SinomoVideoEncoder implements AutoCloseable {
    private MediaCodec codec;
    private Surface surface;
    private Renderer renderer;
    private byte[] configuration;
    private long timestamp;
    private boolean first=true;
    private final Thread owner=Thread.currentThread();
    public SinomoVideoEncoder(int width,int height,int fps) throws Exception {
        try {
            String name=null; int level=0;
            for(MediaCodecInfo info:new MediaCodecList(MediaCodecList.ALL_CODECS).getCodecInfos()) {
                if(!info.isEncoder() || (Build.VERSION.SDK_INT>=29 && (!info.isHardwareAccelerated() || info.isAlias())))continue;
                if(Build.VERSION.SDK_INT<29 && (info.getName().startsWith("OMX.google.") || info.getName().startsWith("c2.android.")))continue;
                for(String type:info.getSupportedTypes())if(type.equals("video/avc")) {
                    MediaCodecInfo.CodecCapabilities caps=info.getCapabilitiesForType(type);
                    if(caps.getVideoCapabilities().areSizeAndRateSupported(width,height,fps)) {
                        for(MediaCodecInfo.CodecProfileLevel profile:caps.profileLevels)
                            if(profile.profile==MediaCodecInfo.CodecProfileLevel.AVCProfileBaseline)level=Math.max(level,profile.level);
                        if(level!=0)name=info.getName();
                    }
                }
                if(name!=null)break;
            }
            if(name==null)throw new IOException("No hardware H264 encoder supports the requested dimensions/rate");
            codec=MediaCodec.createByCodecName(name);
            MediaFormat format=MediaFormat.createVideoFormat("video/avc",width,height);
            format.setInteger(MediaFormat.KEY_COLOR_FORMAT,MediaCodecInfo.CodecCapabilities.COLOR_FormatSurface);
            format.setInteger(MediaFormat.KEY_BIT_RATE,1000000);
            format.setInteger(MediaFormat.KEY_FRAME_RATE,fps);
            format.setInteger(MediaFormat.KEY_I_FRAME_INTERVAL,1);
            format.setInteger(MediaFormat.KEY_PROFILE,MediaCodecInfo.CodecProfileLevel.AVCProfileBaseline);
            format.setInteger(MediaFormat.KEY_LEVEL,level);
            format.setInteger(MediaFormat.KEY_PRIORITY,0);
            format.setInteger("max-bframes",0);
            format.setInteger("latency",0);
            codec.configure(format,null,null,MediaCodec.CONFIGURE_FLAG_ENCODE);
            surface=codec.createInputSurface();codec.start();
            renderer=new Renderer(surface,width,height);
        } catch(Exception error) { close(); throw error; }
    }
    public byte[] encode(ByteBuffer pixels,int width,int height,int format) throws Exception {
        check(Thread.currentThread()==owner,"encoder changed worker thread");
        check(width>0&&height>0&&width<=8192&&height<=8192&&format>=0&&format<=2,"invalid source frame");
        check(pixels.remaining()==(long)width*height*(format==0?3:4),"invalid source length");
        long pts=timestamp=Math.max(timestamp+1,System.nanoTime()/1000);
        renderer.draw(pixels,width,height,format,pts);
        long deadline=System.nanoTime()+1000000000L;
        MediaCodec.BufferInfo info=new MediaCodec.BufferInfo();
        while(System.nanoTime()<deadline) {
            int index=codec.dequeueOutputBuffer(info,1000);
            if(index==MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) {
                MediaFormat output=codec.getOutputFormat();ByteArrayOutputStream csd=new ByteArrayOutputStream();
                for(int i=0;output.containsKey("csd-"+i);i++) {
                    ByteBuffer bytes=output.getByteBuffer("csd-"+i).duplicate();byte[] data=new byte[bytes.remaining()];bytes.get(data);csd.write(data);
                }
                configuration=csd.toByteArray();
            } else if(index>=0) {
                try {
                    if((info.flags&MediaCodec.BUFFER_FLAG_CODEC_CONFIG)!=0 || info.size==0)continue;
                    check(info.presentationTimeUs==pts,"encoder delayed/reordered frame");
                    check(info.size<=32*1024*1024,"encoded frame exceeds limit");
                    boolean key=(info.flags&MediaCodec.BUFFER_FLAG_KEY_FRAME)!=0;
                    check(!first||key,"first encoded frame is not independent");
                    check(configuration!=null&&configuration.length>0&&configuration.length<=65536,"missing codec configuration");
                    ByteBuffer bytes=codec.getOutputBuffer(index).duplicate();bytes.position(info.offset).limit(info.offset+info.size);
                    byte[] result=new byte[info.size+(key?configuration.length:0)];
                    if(key)System.arraycopy(configuration,0,result,0,configuration.length);
                    bytes.get(result,key?configuration.length:0,info.size);first=false;return result;
                } finally { codec.releaseOutputBuffer(index,false); }
            }
        }
        throw new IOException("H264 encoder did not produce a frame within one second");
    }
    public void close() {
        if(renderer!=null){renderer.close();renderer=null;}
        if(codec!=null){try{codec.stop();}catch(Exception ignored){}codec.release();codec=null;}
        if(surface!=null){surface.release();surface=null;}
    }
    static void check(boolean value,String message) throws IOException { if(!value)throw new IOException(message); }
    static final class Renderer implements AutoCloseable {
        EGLDisplay display=EGL14.EGL_NO_DISPLAY; EGLContext context=EGL14.EGL_NO_CONTEXT; EGLSurface window=EGL14.EGL_NO_SURFACE;
        int texture, program; final FloatBuffer vertices; final int width, height;
        Renderer(Surface surface, int width, int height) throws Exception {
            this.width=width; this.height=height;
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
            int fs=shader(GLES20.GL_FRAGMENT_SHADER,"precision mediump float;varying vec2 uv;uniform sampler2D tex;uniform int bgra;void main(){vec4 c=texture2D(tex,uv);gl_FragColor=bgra==1?c.bgra:c;}");
            program=GLES20.glCreateProgram();GLES20.glAttachShader(program,vs);GLES20.glAttachShader(program,fs);GLES20.glLinkProgram(program);
            int[] ok=new int[1];GLES20.glGetProgramiv(program,GLES20.GL_LINK_STATUS,ok,0);check(ok[0]!=0,"shader link");
            GLES20.glDeleteShader(vs);GLES20.glDeleteShader(fs);
            int[] ids=new int[1];GLES20.glGenTextures(1,ids,0);texture=ids[0];GLES20.glBindTexture(GLES20.GL_TEXTURE_2D,texture);
            GLES20.glTexParameteri(GLES20.GL_TEXTURE_2D,GLES20.GL_TEXTURE_MIN_FILTER,GLES20.GL_LINEAR);
            GLES20.glTexParameteri(GLES20.GL_TEXTURE_2D,GLES20.GL_TEXTURE_MAG_FILTER,GLES20.GL_LINEAR);
            } catch (Exception error) { close(); throw error; }
        }
        static int shader(int type,String source) throws Exception {
            int s=GLES20.glCreateShader(type);GLES20.glShaderSource(s,source);GLES20.glCompileShader(s);int[] ok=new int[1];GLES20.glGetShaderiv(s,GLES20.GL_COMPILE_STATUS,ok,0);
            if(ok[0]==0)throw new IOException(GLES20.glGetShaderInfoLog(s));return s;
        }
        void draw(ByteBuffer pixels,int sourceWidth,int sourceHeight,int format,long ptsUs) throws Exception {
            GLES20.glBindTexture(GLES20.GL_TEXTURE_2D,texture);
            GLES20.glPixelStorei(GLES20.GL_UNPACK_ALIGNMENT,1);
            int pixelFormat=format==0?GLES20.GL_RGB:GLES20.GL_RGBA;
            GLES20.glTexImage2D(GLES20.GL_TEXTURE_2D,0,pixelFormat,sourceWidth,sourceHeight,0,pixelFormat,GLES20.GL_UNSIGNED_BYTE,pixels);
            check(GLES20.glGetError()==GLES20.GL_NO_ERROR,"texture upload");
            GLES20.glViewport(0,0,width,height);GLES20.glDisable(GLES20.GL_SCISSOR_TEST);GLES20.glUseProgram(program);GLES20.glUniform1i(GLES20.glGetUniformLocation(program,"bgra"),format==2?1:0);
            int pos=GLES20.glGetAttribLocation(program,"p"),tex=GLES20.glGetAttribLocation(program,"t");
            vertices.position(0);GLES20.glVertexAttribPointer(pos,2,GLES20.GL_FLOAT,false,16,vertices);GLES20.glEnableVertexAttribArray(pos);
            vertices.position(2);GLES20.glVertexAttribPointer(tex,2,GLES20.GL_FLOAT,false,16,vertices);GLES20.glEnableVertexAttribArray(tex);
            GLES20.glDrawArrays(GLES20.GL_TRIANGLE_STRIP,0,4);
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
}
