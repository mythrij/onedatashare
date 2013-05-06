package stork.util;

// A class for keeping track of time in a way that it is guaranteed to
// be monotonically increasing even in the face of system time changes
// and also provide a meaningful time. Also provides a stopwatch
// mechanism to measure elapsed time.

public class Watch {
  private static long abs_base = System.currentTimeMillis();
  private static long rel_base = System.nanoTime() / (long)1E6;

  private volatile long st, et;

  // Create an unstarted watch.
  public Watch() {
    this(-1, -1);
  }

  // Create a watch, and start it now if started is true.
  public Watch(boolean start) {
    this(start ? now() : -1);
  }

  // Create a watch with a given start time.
  public Watch(long start_time) {
    this(start_time, -1);
  }

  // Create a watch with both a given start and end time.
  public Watch(long start_time, long end_time) {
    st = start_time;
    et = end_time;
  }

  // Get the current time since epoch in milliseconds.
  public static long now() {
    return abs_base + (System.nanoTime()/(long)1E6 - rel_base);
  }

  // Get the elapsed time since a given time.
  public static long since(long t) {
    return (t < 0) ? 0 : now()-t;
  }

  // Given a duration in ms, return a pretty string representation.
  public static String pretty(long t) {
    if (t < 0) return null;

    long i = t % 1000,
         s = (t/=1000) % 60,
         m = (t/=60) % 60,
         h = (t/=60) % 24,
         d = t / 24;

    return (d > 0) ? String.format("%dd%02dh%02dm%02ds", d, h, m, s) :
           (h > 0) ? String.format("%dh%02dm%02ds", h, m, s) :
           (m > 0) ? String.format("%dm%02ds", m, s) :
           (s > 0) ? String.format("%d.%02ds", s, i/10) :
                     String.format("%d.%04ds", s, i/10);
  }

  // Start (or restart) the timer.
  public synchronized Watch start() {
    st = now();
    et = -1;
    return this;
  }

  // Get either the current time or the total time if ended. Returns 0
  // if not started.
  public synchronized long elapsed() {
    return (et >= 0) ? et-st : since(st);
  }

  // Stop the timer.
  public synchronized long stop() {
    return (et = now())-st;
  }

  // Get start and end times. Returns -1 if not started/ended.
  public long startTime() { return st; }
  public long endTime()   { return et; }

  // Display the elapsed time as a pretty string.
  public String toString() {
    return pretty(elapsed());
  }
}
