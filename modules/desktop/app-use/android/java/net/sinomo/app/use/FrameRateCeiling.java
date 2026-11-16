package net.sinomo.app.use;

/** Admission count in a rolling second, not a periodic frame clock. */
final class FrameRateCeiling {
    private static final long WINDOW_NS=1_000_000_000L;
    private final long[] admitted;
    private int first, count;
    FrameRateCeiling(int fps) {
        if(fps<1||fps>120)throw new IllegalArgumentException("fps must be 1..120");
        admitted=new long[fps];
    }
    long delayNanos(long now) {
        while(count>0 && now-admitted[first]>=WINDOW_NS) {first=(first+1)%admitted.length;count--;}
        return count<admitted.length?0:WINDOW_NS-(now-admitted[first]);
    }
    void admit(long now) {
        if(delayNanos(now)!=0)throw new IllegalStateException("frame ceiling exhausted");
        admitted[(first+count)%admitted.length]=now;count++;
    }
}
