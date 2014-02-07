package stork.util;

import static stork.util.Watch.now;
import stork.ad.*;

// Used to track transfer progress. Just tell this thing when some
// bytes or a file are done and it will update its state with that
// information.

public class TransferProgress {
  public final transient Watch watch = new Watch();
  public final Progress files = new Progress();
  public final Throughput bytes = new Throughput();

  // Called when a transfer starts.
  public synchronized void transferStarted(long b, int f) {
    watch.start();
    bytes.reset(b);
    files.reset(f);
  }

  // Called when a transfer ends. If successful, assume any unreported
  // bytes and files have finished and report them ourselves.
  public synchronized void transferEnded(boolean success) {
    watch.stop();
    if (success) {
      files.finish();
      bytes.finish(true);
    }
  }

  // Called when some bytes/file have finished transferring.
  public synchronized void done(long b) {
    done(b, 0);
  } public synchronized void done(long b, int f) {
    Log.fine("Bytes done: ", b, "  files done: ", f);
    if (b > 0) bytes.add(b);
    if (f > 0) files.add(f);
  }
}
