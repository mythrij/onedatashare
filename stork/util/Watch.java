package stork.util;

// A class for keeping track of time in a way that it is guaranteed to
// be monotonically increasing even in the face of system time changes
// and also provide a meaningful time. Also provides a stopwatch
// mechanism to measure elapsed time.

public class Watch {
  public volatile long start, end = -1;

  private static long abs_base = System.currentTimeMillis();
  private static long rel_base = System.nanoTime();

  // Create an unstarted watch.
  public Watch() {
    this(-1);
  }

  // Create a watch, and start it now if started is true.
  public Watch(boolean start) {
    this(start ? now() : -1);
  }

  // Create a watch with a given start time.
  public Watch(long start) {
    this.start = start;
  }

  // Get the current time since epoch in milliseconds.
  public static long now() {
    return abs_base + (System.nanoTime() - rel_base)/(long)1E6;
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
                     String.format("%dms", i);
  }

  // Start (or restart) the timer.
  public synchronized Watch start() {
    start = now();
    return this;
  }

  // Returns the elapsed time of this watch either now or at a given
  // time. Will always return a non-negative number.
  public synchronized long elapsed() {
    return elapsed(now());
  } public synchronized long elapsed(long now) {
    if (end  >= 0 && now > end) now = end;
    if (start < 0 || now < start) return 0;
    return now-start;
  }

  // Stop the timer.
  public synchronized long stop() {
    end = now();
    return end-start;
  }

  // Get start and end times. Returns -1 if not started/ended.
  public long startTime() { return start; }
  public long endTime()   { return end; }

  // Check if the timer is started/stopped.
  public boolean isStarted() { return start != -1; }
  public boolean isStopped() { return end   != -1; }

  // Display the elapsed time as a pretty string.
  public String toString() {
    return pretty(elapsed());
  }
}
