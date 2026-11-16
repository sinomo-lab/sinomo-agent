package net.sinomo.app.runtime;

import android.graphics.ImageFormat;
import android.graphics.Rect;
import android.media.Image;
import android.media.MediaCodec;
import android.media.MediaCodecInfo;
import android.media.MediaCodecList;
import android.media.MediaFormat;
import android.os.Build;
import java.io.IOException;
import java.nio.ByteBuffer;

/** Stream-owned hardware decoder. Calls are synchronous and serialized, with no UI or permissions. */
public final class SinomoVideoDecoder implements AutoCloseable {
    private MediaCodec codec;
    private final int width, height;
    private long sequence;
    private MediaFormat outputFormat;
    private DecodedFrame pending;

    public SinomoVideoDecoder(int width, int height) throws Exception {
        if (width < 2 || height < 2 || width > 1920 || height > 1920 || (width & 1) != 0 || (height & 1) != 0)
            throw new IOException("Invalid decoder dimensions");
        this.width = width; this.height = height;
        // Older Android versions cannot positively identify hardware acceleration.
        if (Build.VERSION.SDK_INT < 29) throw new IOException("Hardware decoder identification requires Android 10");
        Exception last = null;
        for (MediaCodecInfo info : new MediaCodecList(MediaCodecList.REGULAR_CODECS).getCodecInfos()) {
            if (info.isEncoder() || !info.isHardwareAccelerated() || info.isSoftwareOnly() || info.isAlias()) continue;
            for (String type : info.getSupportedTypes()) {
                if (!type.equalsIgnoreCase("video/avc")) continue;
                try {
                    MediaCodecInfo.CodecCapabilities caps = info.getCapabilitiesForType(type);
                    if (!caps.getVideoCapabilities().isSizeSupported(width, height)) continue;
                    MediaFormat format = MediaFormat.createVideoFormat(type, width, height);
                    format.setInteger(MediaFormat.KEY_COLOR_FORMAT, MediaCodecInfo.CodecCapabilities.COLOR_FormatYUV420Flexible);
                    if (Build.VERSION.SDK_INT >= 30 && caps.isFeatureSupported(MediaCodecInfo.CodecCapabilities.FEATURE_LowLatency))
                        format.setInteger(MediaFormat.KEY_LOW_LATENCY, 1);
                    codec = MediaCodec.createByCodecName(info.getName());
                    codec.configure(format, null, null, 0);
                    codec.start();
                    android.util.Log.i("SinomoMediaDecoder", "Hardware H264 decoder: " + info.getName());
                    return;
                } catch (Exception error) { last = error; close(); }
            }
        }
        throw new IOException("No compatible hardware H264 decoder", last);
    }

    /** Keeps the output image alive until native RGB conversion finishes. */
    public synchronized DecodedFrame decode(ByteBuffer access) throws Exception {
        if (codec == null || pending != null) throw new IOException("Decoder closed or previous image still owned");
        if (access.remaining() == 0 || access.remaining() > 16 * 1024 * 1024) throw new IOException("Invalid decoder input");
        long pts = ++sequence * 1000;
        long deadline = System.nanoTime() + (sequence == 1 ? 1_000_000_000L : 250_000_000L);
        int input = codec.dequeueInputBuffer(100_000);
        if (input < 0) throw new IOException("Decoder input timeout");
        ByteBuffer buffer = codec.getInputBuffer(input);
        if (buffer == null || buffer.capacity() < access.remaining()) throw new IOException("Decoder input capacity");
        int size = access.remaining(); buffer.clear(); buffer.put(access);
        codec.queueInputBuffer(input, 0, size, pts, 0);
        MediaCodec.BufferInfo info = new MediaCodec.BufferInfo();
        while (System.nanoTime() < deadline) {
            int output = codec.dequeueOutputBuffer(info, 1000);
            if (output == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) { outputFormat = codec.getOutputFormat(); continue; }
            if (output < 0) continue;
            if ((info.flags & MediaCodec.BUFFER_FLAG_CODEC_CONFIG) != 0) { codec.releaseOutputBuffer(output, false); continue; }
            Image image = null;
            try {
                if (info.presentationTimeUs != pts) throw new IOException("Decoder reordered or delayed a frame");
                image = codec.getOutputImage(output);
                if (image == null || image.getFormat() != ImageFormat.YUV_420_888) throw new IOException("Decoder has no flexible YUV image");
                Rect crop = image.getCropRect();
                if (crop.width() != width || crop.height() != height || crop.left < 0 || crop.top < 0 || (crop.left & 1) != 0 || (crop.top & 1) != 0)
                    throw new IOException("Decoder crop differs from stream dimensions");
                MediaFormat format = outputFormat == null ? codec.getOutputFormat() : outputFormat;
                int standard = format.containsKey(MediaFormat.KEY_COLOR_STANDARD) ? format.getInteger(MediaFormat.KEY_COLOR_STANDARD) : MediaFormat.COLOR_STANDARD_BT601_NTSC;
                int range = format.containsKey(MediaFormat.KEY_COLOR_RANGE) ? format.getInteger(MediaFormat.KEY_COLOR_RANGE) : MediaFormat.COLOR_RANGE_LIMITED;
                if (standard == 0) standard = MediaFormat.COLOR_STANDARD_BT601_NTSC;
                if (range == 0) range = MediaFormat.COLOR_RANGE_LIMITED;
                pending = new DecodedFrame(image, output, crop, standard, range);
                return pending;
            } catch (Exception error) {
                if (image != null) image.close();
                codec.releaseOutputBuffer(output, false);
                throw error;
            }
        }
        throw new IOException("Decoder output timeout");
    }

    public final class DecodedFrame implements AutoCloseable {
        private Image image;
        private final int output;
        private final ByteBuffer[] buffers = new ByteBuffer[3];
        private final int[] layout = new int[14];
        private DecodedFrame(Image image, int output, Rect crop, int standard, int range) throws IOException {
            this.image = image; this.output = output;
            Image.Plane[] planes = image.getPlanes();
            if (planes.length != 3) throw new IOException("Invalid YUV planes");
            for (int p = 0; p < 3; p++) {
                int shift = p == 0 ? 0 : 1;
                ByteBuffer buffer = planes[p].getBuffer();
                if (!buffer.isDirect()) throw new IOException("Decoder image is not directly accessible");
                long offset = (long)buffer.position() + (long)(crop.top >> shift) * planes[p].getRowStride()
                    + (long)(crop.left >> shift) * planes[p].getPixelStride();
                if (offset < 0 || offset > buffer.limit()) throw new IOException("Invalid image crop offset");
                buffers[p] = buffer;
                layout[4*p] = (int)offset; layout[4*p+1] = planes[p].getRowStride();
                layout[4*p+2] = planes[p].getPixelStride(); layout[4*p+3] = buffer.limit();
            }
            layout[12] = standard; layout[13] = range;
        }
        public ByteBuffer plane(int index) { return buffers[index]; }
        public int[] layout() { return layout; }
        @Override public void close() {
            synchronized(SinomoVideoDecoder.this) {
                if (image == null) return;
                try { image.close(); } finally {
                    image = null;
                    try { codec.releaseOutputBuffer(output, false); } finally { pending = null; }
                }
            }
        }
    }
    @Override public synchronized void close() {
        if (codec != null) {
            if (pending != null) { try { pending.close(); } catch (Exception ignored) { } }
            try { codec.stop(); } catch (Exception ignored) { }
            try { codec.release(); } finally { codec = null; }
        }
    }
}
