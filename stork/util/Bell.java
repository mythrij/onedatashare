package stork.util;

// A "bell" that can be "rung" to wakeup things waiting on it. :) An
// optional value can be passed to ring() that will be returned to
// threads calling waitFor().

public class Bell<O> {
  O thing = null;

  // Ring the bell to wakeup sleeping threads, optionally with a return
  // value for waitFor().
  public synchronized void ring() {
    ring(null);
  } public synchronized void ring(O o) {
    thing = o;
    notifyAll();
    onRing(o);
  }

  // Wait for the bell to ring, optionally for some maximum duration. In
  // case of interruption, the InterruptException will be wrapped in an
  // Error, so callers can choose to handle it only if necessary.
  public synchronized O waitFor() throws Error {
    return waitFor(0, 0);
  } public synchronized O waitFor(long time) throws Error {
    return waitFor(time, 0);
  } public synchronized O waitFor(long time, int ns) throws Error {
    try {
      wait();
      return thing;
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  // Run when the bell is rung. Intended to be overwritten in
  // subclasses.
  public synchronized void onRing(O o) {
    // Default implementation does nothing.
  }
}
