package net.sinomo.app.use;

import android.graphics.SurfaceTexture;
import android.opengl.*;
import android.os.Handler;
import android.view.Surface;
import java.io.IOException;
import java.nio.*;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import org.json.JSONObject;

/** One projection texture, with demand-driven per-consumer GPU outputs. */
final class AppUseGpuCapture implements AutoCloseable {
    private int nativeWidth, nativeHeight;
    private EGLDisplay display=EGL14.EGL_NO_DISPLAY;
    private EGLContext context=EGL14.EGL_NO_CONTEXT;
    private EGLSurface idle=EGL14.EGL_NO_SURFACE;
    private EGLConfig config;
    private int texture, program;
    private SurfaceTexture source;
    Surface surface;
    private final Set<Client> clients=new HashSet<>();
    private final FloatBuffer vertices=ByteBuffer.allocateDirect(16*4).order(ByteOrder.nativeOrder()).asFloatBuffer();
    private final float[] transform=new float[16];
    private long generation, capturedAt, timestamp;
    private long sourceEpoch=-1, timestampBase;
    private boolean closed, frameReady;
    private final Handler handler;
    private final Runnable onFrame, onFatal;

    AppUseGpuCapture(int width,int height,Handler handler,Runnable onFrame,Runnable onFatal) throws Exception {
        nativeWidth=width;nativeHeight=height;
        this.handler=handler;this.onFrame=onFrame;this.onFatal=onFatal;
        try {
            display=EGL14.eglGetDisplay(EGL14.EGL_DEFAULT_DISPLAY);
            int[] versions=new int[2];check(EGL14.eglInitialize(display,versions,0,versions,1),"eglInitialize");
            EGLConfig[] configs=new EGLConfig[1];int[] count=new int[1];
            int[] attrs={EGL14.EGL_RED_SIZE,8,EGL14.EGL_GREEN_SIZE,8,EGL14.EGL_BLUE_SIZE,8,EGL14.EGL_ALPHA_SIZE,8,
                EGL14.EGL_SURFACE_TYPE,EGL14.EGL_WINDOW_BIT|EGL14.EGL_PBUFFER_BIT,
                EGL14.EGL_RENDERABLE_TYPE,EGL14.EGL_OPENGL_ES2_BIT,0x3142,1,EGL14.EGL_NONE};
            check(EGL14.eglChooseConfig(display,attrs,0,configs,0,1,count,0)&&count[0]>0,"eglChooseConfig");config=configs[0];
            context=EGL14.eglCreateContext(display,config,EGL14.EGL_NO_CONTEXT,new int[]{EGL14.EGL_CONTEXT_CLIENT_VERSION,2,EGL14.EGL_NONE},0);
            idle=pbuffer(1,1);current(idle);
            int vs=shader(GLES20.GL_VERTEX_SHADER,"attribute vec2 p;attribute vec2 t;uniform mat4 transform;varying vec2 uv;void main(){gl_Position=vec4(p,0.,1.);uv=(transform*vec4(t,0.,1.)).xy;}");
            int fs=shader(GLES20.GL_FRAGMENT_SHADER,"#extension GL_OES_EGL_image_external : require\nprecision mediump float;varying vec2 uv;uniform samplerExternalOES tex;uniform int bgra;void main(){vec4 c=texture2D(tex,uv);gl_FragColor=bgra==1?c.bgra:c;}");
            program=GLES20.glCreateProgram();GLES20.glAttachShader(program,vs);GLES20.glAttachShader(program,fs);GLES20.glLinkProgram(program);
            int[] ok=new int[1];GLES20.glGetProgramiv(program,GLES20.GL_LINK_STATUS,ok,0);check(ok[0]!=0,"shader link");
            GLES20.glDeleteShader(vs);GLES20.glDeleteShader(fs);
            createSource(width,height);
        } catch(Exception error) {close();throw error;}
    }
    private void createSource(int width,int height) throws Exception {
            int[] ids=new int[1];GLES20.glGenTextures(1,ids,0);texture=ids[0];GLES20.glBindTexture(GLES11Ext.GL_TEXTURE_EXTERNAL_OES,texture);
            GLES20.glTexParameteri(GLES11Ext.GL_TEXTURE_EXTERNAL_OES,GLES20.GL_TEXTURE_MIN_FILTER,GLES20.GL_LINEAR);
            GLES20.glTexParameteri(GLES11Ext.GL_TEXTURE_EXTERNAL_OES,GLES20.GL_TEXTURE_MAG_FILTER,GLES20.GL_LINEAR);
            GLES20.glTexParameteri(GLES11Ext.GL_TEXTURE_EXTERNAL_OES,GLES20.GL_TEXTURE_WRAP_S,GLES20.GL_CLAMP_TO_EDGE);
            GLES20.glTexParameteri(GLES11Ext.GL_TEXTURE_EXTERNAL_OES,GLES20.GL_TEXTURE_WRAP_T,GLES20.GL_CLAMP_TO_EDGE);
            source=new SurfaceTexture(texture);source.setDefaultBufferSize(width,height);surface=new Surface(source);
            source.setOnFrameAvailableListener(incoming -> {
                if(closed || incoming!=source)return;
                try {
                    current(idle);source.updateTexImage();source.getTransformMatrix(transform);
                    long sourceTime=source.getTimestamp();if(sourceEpoch<0)sourceEpoch=sourceTime;
                    timestamp=Math.max(timestamp+1,timestampBase+Math.max(0,sourceTime-sourceEpoch));
                    capturedAt=System.currentTimeMillis();generation++;frameReady=true;
                    onFrame.run();
                } catch(Exception error) { android.util.Log.e("SinomoAppUse","Projection texture failed",error);close();onFatal.run();onFrame.run(); }
            },handler);
    }
    void resize(int width,int height) throws Exception {
        check(!closed,"projection closed");current(idle);
        // The owner detaches the virtual display first. A fresh input queue
        // cannot deliver old-size buffers after the geometry notification.
        frameReady=false;
        if(source!=null){source.setOnFrameAvailableListener(null);source.release();source=null;}
        if(surface!=null){surface.release();surface=null;}
        GLES20.glDeleteTextures(1,new int[]{texture},0);texture=0;
        nativeWidth=width;nativeHeight=height;sourceEpoch=-1;timestampBase=timestamp+1;
        createSource(width,height);
    }
    Client open(JSONObject request,Function<String,Object> factory) throws Exception {
        check(!closed,"projection closed");
        JSONObject bounds=request.optJSONObject("resolution");
        int bw=bounds==null?nativeWidth:bounds.getInt("width"),bh=bounds==null?nativeHeight:bounds.getInt("height");
        check(bw>0&&bh>0&&bw<=8192&&bh<=8192&&(long)bw*bh<=32*1024*1024,"invalid resolution");
        String format=request.optString("pixel_format","rgba8");if(format.equals("null"))format="rgba8";
        check(format.equals("rgba8")||format.equals("bgra8")||format.equals("h264"),"unsupported capture format");
        int w=nativeWidth,h=nativeHeight;
        if(w>bw||h>bh) {
            if((long)bw*h<=(long)bh*w){h=(int)((long)h*bw/w);w=bw;}
            else {w=(int)((long)w*bh/h);h=bh;}
        }
        if(format.equals("h264")){w=w/2*2;h=h/2*2;}
        check(w>0&&h>0,"resolution too small for source aspect ratio");
        int fps=request.getInt("fps");check(fps>0&&fps<=120,"invalid frame rate");
        Client client=new Client(w,h,format,fps);
        try {
            if(format.equals("h264")) {
                check(factory!=null,"surface encoder unavailable");
                client.encoder=factory.apply(new JSONObject().put("width",w).put("height",h).put("fps",Math.min(fps,60)).toString());
                Surface output=(Surface)client.encoder.getClass().getMethod("inputSurface").invoke(client.encoder);
                client.output=EGL14.eglCreateWindowSurface(display,config,output,new int[]{EGL14.EGL_NONE},0);
            } else {client.output=pbuffer(w,h);client.pixels=ByteBuffer.allocateDirect(w*h*4);}
            check(client.output!=EGL14.EGL_NO_SURFACE,"capture output surface");clients.add(client);return client;
        } catch(Exception error){client.close();throw error;}
    }
    final class Client implements AutoCloseable {
        final int width,height;final String format;
        private EGLSurface output=EGL14.EGL_NO_SURFACE;
        private Object encoder;private ByteBuffer pixels;
        private boolean ended;
        private final FrameRateCeiling ceiling;
        Client(int w,int h,String f,int fps){width=w;height=h;format=f;ceiling=new FrameRateCeiling(fps);}
        long readyDelayMillis(long after,long maximum) {
            if(!frameReady||generation==0||generation<=after)return maximum;
            long nanos=ceiling.delayNanos(System.nanoTime());
            return Math.min(maximum,(nanos+999_999)/1_000_000);
        }
        AppUseProjectionService.Frame frame(long after) throws Exception {
            check(!closed&&!ended,"capture closed");
            if(!frameReady||generation==0||generation<=after)return null;
            long start=System.nanoTime();
            if(ceiling.delayNanos(start)>0)return null;
            ceiling.admit(start);current(output);
            // Readback is top-down; encoder surfaces use the normal GL origin.
            boolean raw=encoder==null;float bottom=raw?1:0,top=raw?0:1;
            vertices.position(0);vertices.put(new float[]{-1,-1,0,bottom,1,-1,1,bottom,-1,1,0,top,1,1,1,top}).position(0);
            // Keep the negotiated stream geometry through rotation/resizing.
            // Native dimensions travel with each frame for coordinate mapping.
            GLES20.glViewport(0,0,width,height);GLES20.glClearColor(0,0,0,1);GLES20.glClear(GLES20.GL_COLOR_BUFFER_BIT);
            double scale=Math.min((double)width/nativeWidth,(double)height/nativeHeight);
            int viewWidth=Math.max(1,(int)(nativeWidth*scale)),viewHeight=Math.max(1,(int)(nativeHeight*scale));
            GLES20.glViewport((width-viewWidth)/2,(height-viewHeight)/2,viewWidth,viewHeight);GLES20.glUseProgram(program);
            GLES20.glActiveTexture(GLES20.GL_TEXTURE0);GLES20.glBindTexture(GLES11Ext.GL_TEXTURE_EXTERNAL_OES,texture);
            GLES20.glUniform1i(GLES20.glGetUniformLocation(program,"tex"),0);
            GLES20.glUniform1i(GLES20.glGetUniformLocation(program,"bgra"),format.equals("bgra8")?1:0);
            GLES20.glUniformMatrix4fv(GLES20.glGetUniformLocation(program,"transform"),1,false,transform,0);
            int pos=GLES20.glGetAttribLocation(program,"p"),uv=GLES20.glGetAttribLocation(program,"t");
            vertices.position(0);GLES20.glVertexAttribPointer(pos,2,GLES20.GL_FLOAT,false,16,vertices);GLES20.glEnableVertexAttribArray(pos);
            vertices.position(2);GLES20.glVertexAttribPointer(uv,2,GLES20.GL_FLOAT,false,16,vertices);GLES20.glEnableVertexAttribArray(uv);
            GLES20.glDrawArrays(GLES20.GL_TRIANGLE_STRIP,0,4);
            byte[] bytes;
            if(raw) {
                pixels.clear();GLES20.glReadPixels(0,0,width,height,GLES20.GL_RGBA,GLES20.GL_UNSIGNED_BYTE,pixels);
                check(GLES20.glGetError()==GLES20.GL_NO_ERROR,"scaled pixel readback");
                bytes=new byte[width*height*4];pixels.position(0);pixels.get(bytes);
            } else {
                long pts=System.nanoTime()/1000;
                check(EGLExt.eglPresentationTimeANDROID(display,output,pts*1000),"encoder timestamp");
                check(EGL14.eglSwapBuffers(display,output),"encoder submission");
                bytes=(byte[])encoder.getClass().getMethod("readEncodedFrame",long.class).invoke(encoder,pts);
            }
            return new AppUseProjectionService.Frame(bytes,width,height,nativeWidth,nativeHeight,format,generation,timestamp,capturedAt,(System.nanoTime()-start)/1000);
        }
        public void close(){
            if(ended)return;ended=true;clients.remove(this);
            try {current(idle);}catch(Exception ignored){}
            if(output!=EGL14.EGL_NO_SURFACE){EGL14.eglDestroySurface(display,output);output=EGL14.EGL_NO_SURFACE;}
            if(encoder!=null){try{((AutoCloseable)encoder).close();}catch(Exception error){android.util.Log.w("SinomoAppUse","Encoder close failed",error);}encoder=null;}
            pixels=null;
        }
    }
    private EGLSurface pbuffer(int w,int h) throws Exception {
        EGLSurface result=EGL14.eglCreatePbufferSurface(display,config,new int[]{EGL14.EGL_WIDTH,w,EGL14.EGL_HEIGHT,h,EGL14.EGL_NONE},0);
        check(result!=EGL14.EGL_NO_SURFACE,"capture pbuffer");return result;
    }
    private void current(EGLSurface surface) throws Exception {check(EGL14.eglMakeCurrent(display,surface,surface,context),"capture eglMakeCurrent");}
    private static int shader(int type,String text) throws Exception {
        int id=GLES20.glCreateShader(type);GLES20.glShaderSource(id,text);GLES20.glCompileShader(id);int[] ok=new int[1];GLES20.glGetShaderiv(id,GLES20.GL_COMPILE_STATUS,ok,0);
        if(ok[0]==0)throw new IOException(GLES20.glGetShaderInfoLog(id));return id;
    }
    private static void check(boolean condition,String error) throws IOException {if(!condition)throw new IOException(error);}
    public void close() {
        if(closed)return;closed=true;
        for(Client client:clients.toArray(new Client[0]))client.close();
        if(source!=null)source.setOnFrameAvailableListener(null);
        if(surface!=null)surface.release();if(source!=null)source.release();
        if(display!=EGL14.EGL_NO_DISPLAY){
            try{current(idle);GLES20.glDeleteProgram(program);GLES20.glDeleteTextures(1,new int[]{texture},0);}catch(Exception ignored){}
            EGL14.eglMakeCurrent(display,EGL14.EGL_NO_SURFACE,EGL14.EGL_NO_SURFACE,EGL14.EGL_NO_CONTEXT);
            if(idle!=EGL14.EGL_NO_SURFACE)EGL14.eglDestroySurface(display,idle);
            if(context!=EGL14.EGL_NO_CONTEXT)EGL14.eglDestroyContext(display,context);
            EGL14.eglReleaseThread();EGL14.eglTerminate(display);
        }
    }
}
