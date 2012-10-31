package stork.util;

// Used to track transfer progress. All times here are in ms.

public class TransferProgress {
  private long start_time = -1, end_time = -1;
  private long bytes_done = 0, bytes_total = 0;
  private int  files_done = 0, files_total = 1;

  // Metrics used to calculate instantaneous throughput.
  private double q = 1000.0;  // Time quantum for throughput.
  private long qr_time = -1;  // Time the quantum was last reset.
  private long btq = 0;  // bytes this quantum
  private long blq = 0;  // bytes last quantum

  // Get the current time in ms.
  public static long now() {
    return System.nanoTime() / (long) 1E6;
  }

  // Pretty format a throughput.
  public static String prettyThrp(double tp, char pre) {
    if (tp >= 1000) switch (pre) {
      case ' ': return prettyThrp(tp/1000, 'k');
      case 'k': return prettyThrp(tp/1000, 'M');
      case 'M': return prettyThrp(tp/1000, 'G');
      case 'G': return prettyThrp(tp/1000, 'T');
    } return String.format("%.2f%cB/sec", tp, pre);
  }

  // Pretty format a duration (in milliseconds);
  public static String prettyTime(long t) {
    if (t < 0) return null;

    long i = t % 1000,
         s = (t/=1000) % 60,
         m = (t/=60) % 60,
         h = (t/=60) % 24,
         d = t / 24;

    return (d > 0) ? String.format("%dd%02dh%02dm%02ds", d, h, m, s) :
           (h > 0) ? String.format("%dh%02dm%02ds", h, m, s) :
           (m > 0) ? String.format("%dm%02ds", m, s) :
                     String.format("%d.%02ds", s, i/10);
  }

  // Can be used to change time quantum. Minimum 1ms.
  public void setQuantum(double t) {
    q = (t < 1.0) ? 1.0 : t;
    btq = blq = 0;
  }

  // Called when some number of bytes have finished transferring.
  public void bytesDone(long bytes) {
    long now = now();
    long diff = 0;

    if (qr_time == -1)
      qr_time = now;
    else
      diff = now-qr_time;

    // See if we need to reset time quantum.
    if (diff > q) {
      System.out.println("Resetting btq...");
      System.out.println("diff/q : "+diff+"/"+q);
      System.out.println("rlq = "+btq);
      blq = (long) (btq * q / diff);
      btq = 0;
      qr_time = now;
      System.out.println("blq = "+blq);
    }

    bytes_done += bytes;

    if (bytes_done > bytes_total)
      bytes_done = bytes_total;
    else
      btq += bytes;
  }

  // Called when a file has finished transferring.
  public void fileDone(long bytes) {
    if (++files_done > files_total)
      files_done = files_total;
    bytesDone(bytes);
  }

  // Called when a transfer starts.
  public void transferStarted() {
    if (start_time == -1)
      start_time = now();
  }

  // Called when a transfer ends.
  public void transferEnded() {
    if (end_time == -1)
      end_time = now();
  }

  // Get the throughput in bytes per second. When doing instantaneous
  // throughput, takes current time quantum as well as last quantum into
  // account (the last scaled by how far into this quantum we are).
  public double throughputValue(boolean avg) {
    double d;  // Duration in ms.
    long b;  // Bytes over duration.
    long now = now();

    if (avg) {  // Calculate average
      if (start_time == -1)
        return 0.0;
      if (end_time == -1)
        d = (double) (now-start_time);
      else
        d = (double) (end_time-start_time);
      b = bytes_done;
    } else {  // Calculate instantaneous
      if (qr_time == -1)
        return 0.0;
      d = (double) (now-qr_time);
      if (d >= 2*q)
        b = 0;
      else if (d >= q)
        b = btq;
      else
        b = (long) (blq*((q-d)/q) + btq);
      d = q;
    }

    if (d <= 0.0)
      return 0.0;
    return b/d*1000.0;
  }

  public String throughput(boolean avg) {
    double t = throughputValue(avg);
    return (t > 0) ? prettyThrp(t, ' ') : null;
  }

  // Get the duration of the transfer in milliseconds.
  public long durationValue() {
    if (start_time == -1)
      return 0;
    if (end_time == -1)
      return now()-start_time;
    return end_time-start_time;
  }

  // Get the duration as a string.
  public String duration() {
    long t = durationValue();
    return (t > 0) ? prettyTime(t) : null;
  }

  // Byte progress accessors...
  public long bytesDone() {
    return bytes_done;
  }

  public long bytesTotal() {
    return bytes_total;
  }

  public void setBytes(long t) {
    if (t >= 0)
      bytes_total = t;
    if (bytes_done > bytes_total)
      bytes_done = bytes_total;
  }

  public String byteProgress() {
    return bytes_done+"/"+bytes_total;
  }

  public String progress() {
    if (bytes_total == 0)
      return null;
    return String.format("%.2f%%", 100.0*bytes_done/bytes_total);
  }

  // File progress accessors...
  public int filesDone() {
    return files_done;
  }

  public int filesTotal() {
    return files_total;
  }

  public void setFiles(int t) {
    if (t >= 0)
      files_total = t;
    if (files_done > files_total)
      files_done = files_total;
  }

  public String fileProgress() {
    return files_done+"/"+files_total;
  }
}
