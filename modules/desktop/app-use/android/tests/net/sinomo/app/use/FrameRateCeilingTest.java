package net.sinomo.app.use;

/** Run with javac/java; no device or Android framework needed. */
public final class FrameRateCeilingTest {
    private static void equal(long expected, long actual) {
        if (expected != actual) throw new AssertionError(expected + " != " + actual);
    }
    public static void main(String[] args) {
        FrameRateCeiling ceiling = new FrameRateCeiling(3);
        for (long ms : new long[]{0, 10, 700}) {
            equal(0, ceiling.delayNanos(ms * 1_000_000));
            ceiling.admit(ms * 1_000_000);
        }
        equal(200_000_000, ceiling.delayNanos(800_000_000));
        try {
            ceiling.admit(800_000_000);
            throw new AssertionError("exhausted ceiling admitted a frame");
        } catch (IllegalStateException expected) { }
        equal(0, ceiling.delayNanos(1_000_000_000));
        ceiling.admit(1_000_000_000);
        equal(9_000_000, ceiling.delayNanos(1_001_000_000));
        equal(0, ceiling.delayNanos(10_000_000_000L));
        // Wrap the ring repeatedly, including nanoTime's signed wraparound.
        ceiling = new FrameRateCeiling(1);
        long start = Long.MAX_VALUE - 500_000_000;
        for (int i = 0; i < 1000; i++) {
            long now = start + i * 1_000_000_000L;
            equal(0, ceiling.delayNanos(now));
            ceiling.admit(now);
            equal(1_000_000_000, ceiling.delayNanos(now));
        }
        System.out.println("FrameRateCeiling tests passed");
    }
}
