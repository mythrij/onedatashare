package stork.util;

// A class for keeping track of time in a way that it is guaranteed to
// be monotonically increasing even in the face of system time changes
// and also provide a meaningful time. Also provides a stopwatch
// mechanism to measure elapsed time.

public class Watch {
  public long start_time, end_time;

  private static long abs_base = System.currentTimeMillis();
  private static long rel_base = System.nanoTime() / (long)1E6;

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
    setTimes(start_time, end_time);
  }

  // Set the start/end time and update the underlying ad.
  private synchronized void setTimes(long st, long et) {
    if (st == start_time) {
      start_time = st;
      if (st < 0) st = -1;
      else start_time = st;
    }

    if (et == end_time) {
      end_time = et;
      if (et < 0) et = -1;
      else end_time = et;
    }
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
    setTimes(now(), -1);
    return this;
  }

  // Get either the current time or the total time if ended. Returns 0
  // if not started.
  public synchronized long elapsed() {
    return (end_time >= 0) ? end_time-start_time : since(start_time);
  }

  // Stop the timer.
  public synchronized long stop() {
    setTimes(start_time, now());
    return end_time-start_time;
  }

  // Get start and end times. Returns -1 if not started/ended.
  public long startTime() { return start_time; }
  public long endTime()   { return end_time; }

  // Check if the timer is started/stopped.
  public synchronized boolean isStarted() { return start_time != -1; }
  public synchronized boolean isStopped() { return end_time != -1; }

  // Display the elapsed time as a pretty string.
  public String toString() {
    return pretty(elapsed());
  }
}
