package stork.util;

import static stork.util.Watch.now;

// A class for keeping track of average and instantaneous byte throughput.
// FIXME: Instantaneous throughput calculation is a little chungy. Let's
// revisit this are learning a bit more about statistics shall we?
// TODO: Take a look at the synchronization stuff in this class.

public class Throughput extends Progress {
  private transient Watch watch = new Watch();

  // These are stored after each update to make for easy serializability.
  // In the future, when the ad marshalling code is less crap, we can
  // just generate this on the fly. Don't be a baddie and change these
  // from calling classes.
  public long avg = -1, inst = -1;

  // Metrics used to calculate instantaneous throughput.
  private transient long q = 500;  // Time quantum for throughput in ms.
  private transient long lst =-1;  // last sample time
  private transient long btq = 0;  // bytes this quantum
  private transient long blq = 0;  // bytes last quantum

  // Pretty format a throughput.
  public String pretty() {
    return pretty(avg, ' ');
  } public static String pretty(double tp) {
    return pretty(tp, ' ');
  } private static String pretty(double tp, char pre) {
    if (tp < 0)
      return "--";
    return StorkUtil.prettySize(tp)+"B/s";
  }

  // Can be used to change time quantum. Minimum 1ms.
  public synchronized void setQuantum(long t) {
    q = (t < 1) ? 1 : t;
    btq = blq = 0;
  }

  // This should be called when the transfer starts.
  public void reset(long t) {
    super.reset(t);
    watch = new Watch(true);
  }

  // Called when a transfer ends. If successful, assume any unreported
  // bytes and files have finished and report them ourselves.
  public synchronized void finish(boolean success) {
    watch.stop();
    if (success) super.finish();

    // Assumably this will be called by finish, but let's do it anyway so
    // we don't need to debug this later.
    long now = now();
    inst = calculateInstantaneous(0, now);
    avg  = calculateAverage(now);
  }

  // Intercept calls to add so we can also adjust throughput.
  public void add(long d, long t) {
    super.add(d, t);
    long now = now();
    inst = calculateInstantaneous(d, now);
    avg  = calculateAverage(now);
  }

  // Update instantaneous throughput bookkeeping information. Get the
  // throughput in bytes per second. Takes current time quantum as well
  // as last quantum into account (the last scaled by how far into this
  // quantum we are). I'll admit it, I came up with this algorithm one
  // night and I'm pretty sure it's garbage. This has to be a solved
  // problem already, just wish I knew where to find it.
  public long calculateInstantaneous(long b, long now) {
    // The time since the last quantum in ms.
    long d = now-lst;
    double q = (double) this.q;

    // If we've ended, instantanous throughput means nothing. Also go
    // ahead and catch any messery.
    if (watch.isStopped() || now < 0 || d < 0)
      return -1;

    // Update the sampling bookkeeping.
    if (lst == -1) {  // If we've done no samples...
      btq = b;  // this is the first sample.
    } else if (d >= 2*q) {  // If we're beyond the second quantum...
      blq = btq = (long) (b*(q/d));  // throw out all the bytes.
    } else if (d >= q) {  // If we're in the second quantum...
      blq = (long) (b*(1-q/d) + btq*(2-d/q));
      btq = (long) (b*(q/d));
    } else {  // If we're in the current quantum...
      blq = (long) (blq*(1-d/q) + btq*(d/q));
      btq = (long) (btq*(1-d/q) + b);
    } lst = now;  // Save the last sample time.

    // Now calculate the instantaneous throughput.
    b = (long) (btq*(d/q) + blq*(1-d/q));

    return (b <= 0 || d <= 0) ? 0 : b*1000/d;
  }

  // This one is easy. Calculate the average throughput.
  public long calculateAverage(long now) {
    long d = watch.elapsed(now);  // ms
    long b = done;
    return (b <= 0 || d <= 0) ? 0 : b*1000/d;
  }

  // Get the duration of the transfer in milliseconds.
  public long duration() {
    return watch.elapsed(now());
  }
}
