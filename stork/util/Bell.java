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

  // Thrown by reject() to prevent ringing of the bell.
  private static final RuntimeException REJECT = new RuntimeException();

  // These bells will be rung after this bell rings.
  private Set<Bell<T super O>> thens;

  public Bell() { }

  // Create an already-rung bell.
  public Bell(O o) {
    this.o = o;
    state = RUNG;
  }

  // Resolve the bell with a value or exception.
  public boolean ring() {
    return ring(null, null);
  } public boolean ring(Throwable t) {
    return ring(null, t);
  } public boolean ring(O o) {
    return ring(o, null);
  } public boolean ring(O o, Throwable t) {
    return startRing(o, t) ? endRing() : false;
  }

  // This checks the filters and begins ringing the bell.
  protected synchronized boolean startRing(O o, Throwable t) {
    // Make darn sure the bell only rings once.
    if (state() != UNRUNG)
      throw new Error("this bell cannot be rung");
    if (!filter(o, t))
      return false;
    state = RINGING;
    this.o = o;
    this.t = t;
    return true;
  }

  // Call the end handlers. Always returns true.
  protected void endRing() {
    // Run the handlers, and make sure to catch anything they may throw.
    if (state() != RINGING) {
      throw new Error("bell has not begun ringing");
    } if (t != null) try {
      fail(t);
    } catch (RuntimeException e) {
      t = e;
    } else try {
      done(this.o = o);
    } catch (RuntimeException e) {
      fail(t = e);
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

  // Check if we can write this to the pipeline.
  protected boolean ready() {
    return true;
  }

  // A bell which will ring when the passed bells have been rung.
  public static class And extends Bell {
    private Set<Bell> bells;

    public And(Bell... bells) {
      this(Arrays.asList(base));
    } public And(Collection<Bell> bells) {
      this.bells = new HashSet<Bell>();
      this.bells.addAll(bs);
      for (Bell b : this.bells)
        b.then(this);
    }

    boolean andRing(Bell b) {
      if (!isRung()) {
        bells.remove(b);
        if (bells.isEmpty()) return ring();
      } return false;
    }
  }

  // An async handlers which can intercept and modify the result of ringing the
  // bell. Subclasses should access the stored result using get(), which will
  // throw if the bell was rung with a throwable. Throwing from this method
  // will cause the bell to fail. Returning will cause the bell to be resolved
  // with the returned object. Calling reject() will cause ring() -- the only
  // method from which this method should be called -- to return false and the
  // bell to remain unrung.
  protected O onRing() {
    return get();
  }

  // Used in onRing() to reject the ringing object.
  protected final reject() { throw REJECT; }

  // A "thenned" bell will be rung when this bell is rung. If the bell is
  // already rung when the thenned bell is attached, the thenned bell will
  // be rung.
  public synchronized void then(Bell<T super O>... bells) {
    then(Arrays.asList(bells));
  } public synchronized void then(Collection<Bell<T super O>> bells) {
    if (isRung()) for (Bell b : bells) {
      b.ring(o, t);
    } else {
      if (thens == null)
        thens = new HashSet<Bell<T super O>>();
      thens.addAll(bells);
    }
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
