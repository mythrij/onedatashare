package stork.feather.util;

/**
 * A utility for checking time. This class keeps track of time in a way such
 * that it is guaranteed to be monotonically increasing even in the face of
 * system time changes, and also provide a meaningful time.
 */
public class Time {
  private static long absoluteBase = System.currentTimeMillis();
  private static long relativeBase = System.nanoTime();

  /**
   * A timer. A timer is a specialized type of clock for measuring time
   * intervals. A timer which counts upwards from zero for measuring elapsed
   * time is often called a <i>stopwatch</i>, which is unfortunately four
   * characters longer than the word "timer" and is hence not the name of this
   * class.
   */
  public static class Timer {
    private volatile long start = now(), total = 0;

    /** Get the elapsed time of this {@code Timer}. */
    public synchronized long elapsed() {
      return running() ? total : now()-start + total;
    }

    /** Pause the {@code Timer}. Has no effect if stopped. */
    public synchronized void pause() {
      if (!running()) return;
      total += now()-start;
      start = -1;
    }

    /** Resume the {@code Timer}. Has no effect if running. */
    public synchronized void resume() {
      if (!running()) start = now();
    }

    /** Check if the {@code Timer} is running. */
    public synchronized boolean running() { return start > 0; }

    public String toString() { return format(elapsed()); }
  }

  /** Get the current Unix time in milliseconds. */
  public static long now() {
    return absoluteBase + (System.nanoTime() - relativeBase) / (long)1E6;
  }

  /** Get the elapsed time since a given time (in milliseconds). */
  public static long since(long time) {
    return (time < 0) ? 0 : now()-time;
  }

  /** Get the remaining time until a given time (in milliseconds). */
  public static long until(long time) {
    return (time < 0) ? 0 : time-now();
  }

  /** Format a time (in milliseconds) for human readability. */
  public static String format(long time) {
    if (time < 0) return "-"+format(-time);

    long i = (time)%1000,      // Milliseconds
         s = (time/=1000)%60,  // Seconds
         m = (time/=60)%60,    // Minutes
         h = (time/=60)%24,    // Hours
         d = (time)/24;        // Days

    return (d > 0) ? String.format("%dd%02dh%02dm%02ds", d, h, m, s) :
           (h > 0) ? String.format("%dh%02dm%02ds", h, m, s) :
           (m > 0) ? String.format("%dm%02ds", m, s) :
           (s > 0) ? String.format("%d.%02ds", s, i/10) :
                     String.format("%dms", i);
  }
}
