package stork.util;

// A "bell" that can be "rung" to wakeup things waiting on it. An
// optional value can be passed to ring() that will be returned to
// threads calling waitFor().
//
// A bell can be extended to include handlers that execute depending on
// what the bell was rung with. Any of the handlers may throw a runtime
// exception, which will cause the bell to store the exception and pass
// it on to anything waiting on the bell.
//
// Subclasses can also override the filter method to intercept objects,
// potentially modify or act on them, or prevent them from ringing the bell.

public class Bell<O> {
  public static final int UNRUNG  = 0;
  public static final int RINGING = 1;
  public static final int RUNG    = 2;
  private volatile int state = UNRUNG;

  private O o;
  private Throwable t;

  public Bell() { }

  // Create an already-rung bell.
  public Bell(O o) {
    this.o = o;
    state = RUNG;
  }

  // Ring the bell to wakeup sleeping threads, optionally with a return
  // value for waitFor(). This will run the rung objects through filter()
  // before actually ringing the bell. Returns whether or not filter() let
  // the call ring the bell.
  public synchronized boolean ring() {
    return ring(null, null);
  } public boolean ring(Throwable t) {
    return ring(null, t);
  } public boolean ring(O o) {
    return ring(o, null);
  } public boolean ring(O o, Throwable t) {
    boolean b = startRing(o, t);
    if (b) endRing();
    return b;
  }

  // These can be used for more fine-grained timing requirements. This
  // checks the filters and begins ringing the bell.
  public synchronized boolean startRing(O o, Throwable t) {
    // Make darn sure the bell only rings once.
    if (state() != UNRUNG)
      throw new Error("this bell cannot be run again");
    if (!filter(o, t))
      return false;
    state = RINGING;
    this.o = o;
    this.t = t;
    return true;
  }

  // Call the end handlers.
  public void endRing() {
    // Run the handlers, and make sure to catch anything they may throw.
    if (state() != RINGING) {
      throw new Error("bell has not begun ringing");
    } if (t != null) try {
      fail(o, t);
    } catch (RuntimeException e) {
      t = e;
    } else try {
      done(this.o = o);
    } catch (RuntimeException e) {
      fail(o, t = e);
    } try {
      always(o, t);
    } catch (RuntimeException e) {
      t = e;
    } state = RUNG;

    synchronized (this) { notifyAll(); }
  }

  // Handles either getting the object or throwing the throwable. Only
  // call this once the bell has completed being rung. It will busy wait
  // otherwise, just to punish you.
  protected O innerGet() {
    while (state() != RUNG);
    if (t != null) throw (t instanceof RuntimeException) ?
      (RuntimeException) t : new RuntimeException(t);
    return o;
  }

  // Wait for the bell to ring, optionally for some maximum duration.
  public O waitFor() {
    try {  // This will never happen, but Java doesn't believe that.
      return waitFor(false);
    } catch (InterruptedException e) {
      throw new Error("the impossible has happened", e);
    }
  } public O waitFor(boolean interruptable) throws InterruptedException {
    synchronized (this) {
      while (state() != RUNG) try {
        wait();
      } catch (InterruptedException e) {
        if (interruptable) throw e;
      } return innerGet();
    }
  }

  // Check the bell state.
  protected synchronized int state() {
    return state;
  } public boolean isRung() {
    return state() != UNRUNG;
  }

  // This can be implemented by subclasses to intercept incoming objects and
  // protentially prevent them from causing the bell to be rung. It should
  // return true if the object should ring the bell, false otherwise.
  public boolean filter(O o, Throwable t) {
    return true;
  }

  // Check if we can write this to the pipeline.
  protected boolean ready() {
    return true;
  }

  // Handlers which can optionally be overriden by subclasses.
  protected void done(O o) {
    // Called on success, with the ringing object. Can throw if something's
    // wrong, which will trigger fail and alter the stored throwable.
  } protected void fail(O o, Throwable t) {
    // Called on failure (or if done throws). Can throw to alter the stored
    // throwable.
  } protected void always(O o, Throwable t) {
    // Called always, after everything else has been called. Can throw to
    // alter the stored throwable.
  }

  // A bell link can be used to create a new bell from this one which will
  // wait for this one to ring. It must implement a method that transforms
  // the parent bell's object into the right type.
  public abstract class Link<T> extends Bell<T> {
    public boolean ring(T o, Throwable t) {
      throw new Error("linked bells should never be rung");
    } protected T innerGet() {
      return transform(Bell.this.innerGet());
    } public synchronized int state() {
      return Bell.this.state();
    } public abstract T transform(O o);
  }
}
