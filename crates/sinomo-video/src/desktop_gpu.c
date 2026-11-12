/* Private adapter: CPU pixels -> hardware H264. No device capture or networking.
 * All allocations are owned by one stream and released on its worker thread. */
#include <libavcodec/avcodec.h>
#include <libavutil/hwcontext.h>
#include <libavutil/opt.h>
#include <libswscale/swscale.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    AVCodecContext *codec;
    AVFrame *pixels;
    AVPacket *packet;
    AVBufferRef *device;
    struct SwsContext *scale;
    int64_t pts;
} SinomoGpu;

void sinomo_gpu_close(SinomoGpu *s) {
    if (!s) return;
    sws_freeContext(s->scale);
    av_packet_free(&s->packet);
    av_frame_free(&s->pixels);
    avcodec_free_context(&s->codec);
    av_buffer_unref(&s->device);
    free(s);
}

SinomoGpu *sinomo_gpu_open(const char *name, int width, int height, int fps, int *error) {
    int result = AVERROR(ENOMEM);
    SinomoGpu *s = calloc(1, sizeof(*s));
    const AVCodec *codec = avcodec_find_encoder_by_name(name);
    if (!s) goto fail;
    if (!codec) { result = AVERROR_ENCODER_NOT_FOUND; goto fail; }
    s->codec = avcodec_alloc_context3(codec);
    s->pixels = av_frame_alloc();
    s->packet = av_packet_alloc();
    if (!s->codec || !s->pixels || !s->packet) goto fail;
    AVCodecContext *c = s->codec;
    c->width = width; c->height = height;
    c->time_base = (AVRational){1, fps}; c->framerate = (AVRational){fps, 1};
    c->pix_fmt = AV_PIX_FMT_NV12;
    c->profile = AV_PROFILE_H264_BASELINE;
    c->max_b_frames = 0; c->gop_size = fps * 2;
    c->bit_rate = (int64_t)width * height * fps / 5;
    if (c->bit_rate < 500000) c->bit_rate = 500000;
    c->flags |= AV_CODEC_FLAG_LOW_DELAY;
    if (!strcmp(name, "h264_vaapi")) {
        result = av_hwdevice_ctx_create(&s->device, AV_HWDEVICE_TYPE_VAAPI, NULL, NULL, 0);
        if (result < 0) goto fail;
        c->hw_frames_ctx = av_hwframe_ctx_alloc(s->device);
        if (!c->hw_frames_ctx) { result = AVERROR(ENOMEM); goto fail; }
        AVHWFramesContext *frames = (AVHWFramesContext *)c->hw_frames_ctx->data;
        frames->format = AV_PIX_FMT_VAAPI; frames->sw_format = AV_PIX_FMT_NV12;
        frames->width = width; frames->height = height; frames->initial_pool_size = 4;
        result = av_hwframe_ctx_init(c->hw_frames_ctx);
        if (result < 0) goto fail;
        c->pix_fmt = AV_PIX_FMT_VAAPI;
    }
    AVDictionary *options = NULL;
    if (!strcmp(name, "h264_vaapi")) {
        av_dict_set(&options, "async_depth", "1", 0);
        av_dict_set(&options, "profile", "constrained_baseline", 0);
    } else if (!strcmp(name, "h264_nvenc")) {
        av_dict_set(&options, "preset", "p1", 0);
        av_dict_set(&options, "tune", "ull", 0);
        av_dict_set(&options, "zerolatency", "1", 0);
        av_dict_set(&options, "delay", "0", 0);
    } else if (!strcmp(name, "h264_qsv")) {
        av_dict_set(&options, "async_depth", "1", 0);
        av_dict_set(&options, "look_ahead", "0", 0);
    } else if (!strcmp(name, "h264_amf")) {
        av_dict_set(&options, "usage", "ultralowlatency", 0);
    } else if (!strcmp(name, "h264_videotoolbox")) {
        av_dict_set(&options, "realtime", "1", 0);
        av_dict_set(&options, "allow_sw", "0", 0);
        av_dict_set(&options, "max_frame_delay", "0", 0);
    }
    result = avcodec_open2(c, codec, &options);
    if (result >= 0 && av_dict_count(options)) result = AVERROR(EINVAL);
    av_dict_free(&options);
    if (result < 0) goto fail;
    s->pixels->format = AV_PIX_FMT_NV12;
    s->pixels->width = width; s->pixels->height = height;
    result = av_frame_get_buffer(s->pixels, 32);
    if (result < 0) goto fail;
    return s;
fail:
    *error = result;
    sinomo_gpu_close(s);
    return NULL;
}

/* Output borrows packet memory until the next call or close. */
int sinomo_gpu_encode(SinomoGpu *s, const unsigned char *data, int width,
                      int height, int stride, int format,
                      const unsigned char **output, int *length) {
    enum AVPixelFormat pixel_format = format == 0 ? AV_PIX_FMT_RGB24 :
                                     format == 1 ? AV_PIX_FMT_RGBA : AV_PIX_FMT_BGRA;
    int result = av_frame_make_writable(s->pixels);
    if (result < 0) return result;
    s->scale = sws_getCachedContext(s->scale, width, height, pixel_format,
        s->codec->width, s->codec->height, AV_PIX_FMT_NV12, SWS_BILINEAR, NULL, NULL, NULL);
    if (!s->scale) return AVERROR(ENOMEM);
    const uint8_t *planes[4] = {data, NULL, NULL, NULL};
    int strides[4] = {stride, 0, 0, 0};
    result = sws_scale(s->scale, planes, strides, 0, height,
                       s->pixels->data, s->pixels->linesize);
    if (result != s->codec->height) return AVERROR(EINVAL);
    s->pixels->pts = s->pts++;
    AVFrame *input = s->pixels;
    AVFrame *hardware = NULL;
    if (s->codec->hw_frames_ctx) {
        hardware = av_frame_alloc();
        if (!hardware) return AVERROR(ENOMEM);
        result = av_hwframe_get_buffer(s->codec->hw_frames_ctx, hardware, 0);
        if (result >= 0) result = av_hwframe_transfer_data(hardware, s->pixels, 0);
        if (result < 0) { av_frame_free(&hardware); return result; }
        hardware->pts = s->pixels->pts;
        input = hardware;
    }
    result = avcodec_send_frame(s->codec, input);
    av_frame_free(&hardware);
    if (result < 0) return result;
    av_packet_unref(s->packet);
    result = avcodec_receive_packet(s->codec, s->packet);
    if (result < 0) return result; /* Delayed encoders cannot satisfy this contract. */
    if (s->packet->pts != s->pixels->pts) return AVERROR(EINVAL);
    *output = s->packet->data; *length = s->packet->size;
    return 0;
}
