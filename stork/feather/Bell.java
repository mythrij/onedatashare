package stork.feather;

import java.util.*;
import java.util.concurrent.*;

/**
 * A promise primitive used for stringing together the results of asynchronous
 * operations and executing asynchronous handlers. It supports callbacks
 * defined in subclasses, chaining of results, deadlines, and
 * back-cancellation.
 *
 * @param <T> the supertype of objects that can ring this {@code Bell}.
 */
public class Bell<T> implements Future<T> {
  private T object;
  private Throwable error;
  private boolean done = false;
  private List<Bell<? super T>> promises = Collections.emptyList();

  // This is used to schedule bell deadlines.
  private static ScheduledExecutorService deadlineTimerPool =
    new ScheduledThreadPoolExecutor(1);

  /** Create an unrung {@code Bell}. */
  public Bell() { }

  /** Create a {@code Bell} rung with {@code object}. */
  public Bell(T object) { ring(object); }

  /** Create a {@code Bell} rung with {@code error}. */
  public Bell(Throwable error) { ring(error); }

  /**
   * Ring the {@code Bell} with the given object and wake up any waiting
   * threads.
   *
   * @param object The object to ring the {@code Bell} with.
   * @return This {@code Bell}.
   */
  public final Bell<T> ring(T object) {
    return ring(object, null);
  }

  /**
   * Ring the {@code Bell} with null and wake up any waiting threads.
   *
   * @return This {@code Bell}.
   */
  public final Bell<T> ring() {
    return ring(null, null);
  }

  /**
   * Ring the {@code Bell} with the given error and wake up any waiting
   * threads.
   *
   * @param error The error to ring the {@code Bell} with.
   * @return This {@code Bell}.
   */
  public final Bell<T> ring(Throwable error) {
    return ring(null, (error != null) ? error : new NullPointerException());
  }

  /**
   * Used by the other ring methods. If error is not null, assume failure.
   *
   * @param object The object to ring the {@code Bell} with.
   * @param error The error to ring the {@code Bell} with.
   * @return This {@code Bell}.
   */
  private Bell<T> ring(T object, Throwable error) {
    List<Bell<? super T>> proms;

    synchronized (this) {
      if (done)
        return this;
      done = true;
      this.object = object;
      this.error  = error;
      proms = promises;
      promises = null;  // Take the set away so promise() blocks.
    }

    // Call the handlers.
    if (error != null) try {
      fail(error);
    } catch (Throwable t) {
      // Discard.
    } else try {
      done(object);
    } catch (Throwable t) {
      // Discard.
    } try {
      always();
    } catch (Throwable t) {
      // Discard.
    }

    // Pass along to all the promises, then put an empty list back.
    for (Bell<? super T> b : proms)
      b.then(object, error);
    promises = Collections.emptyList();

    synchronized (this) {
      notifyAll();
    }

    return this;
  }

  /**
   * Cancel the {@code Bell}, resolving it with a cancellation error. This is
   * here to satisfy the requirements of the {@code Future} interface.
   *
   * @param mayInterruptIfRunning Ignored in this implementation.
   * @return {@code true} if the {@code Bell} was cancelled as a result of this call,
   * {@code false} otherwise.
   * @see Future#cancel(boolean)
   */
  public synchronized boolean cancel(boolean mayInterruptIfRunning) {
    if (done)
      return false;
    ring(null, new CancellationException());
    return true;
  }

  /**
   * Cancel the {@code Bell}, resolving it with a cancellation error. This
   * returns a reference to this {@code Bell}, unlike {@link #cancel(boolean)}.
   *
   * @return This {@code Bell}.
   */
  public Bell<T> cancel() {
    cancel(true);
    return this;
  }

  /**
   * @return {@code true} if this {@code Bell} was rung with a {@code
   * CancellationException}.
   * @see Future#isCancelled
   */
  public synchronized boolean isCancelled() {
    if (!done)
      return false;
    return error != null && error instanceof CancellationException;
  }

  /** Return true if the {@code Bell} has been rung. */
  public synchronized boolean isDone() {
    return done;
  }

  /** Return true if the {@code Bell} rang successfully. */
  public synchronized boolean isSuccessful() {
    return done && error == null;
  }

  /** Return true if the {@code Bell} failed. */
  public synchronized boolean isFailed() {
    return done && error != null;
  }

  /** Wait for the {@code Bell} to be rung, then return the value. */
  public synchronized T get()
  throws InterruptedException, ExecutionException {
    while (!done)
      wait();
    return getOrThrow();
  }

  /**
   * Wait for the {@code Bell} to be rung up to the specified time, then return the
   * value.
   */
  public synchronized T get(long timeout, TimeUnit unit)
  throws InterruptedException, ExecutionException, TimeoutException {
    if (!done)
      unit.timedWait(this, timeout);
    if (!done)
      throw new TimeoutException();
    return getOrThrow();
  }

  /** Either get the object or throw the wrapped error. Only call if done. */
  private T getOrThrow() throws ExecutionException {
    if (error == null)
      return object;
    if (error instanceof CancellationException)
      throw (CancellationException) error;
    if (error instanceof ExecutionException)
      throw (ExecutionException) error;
    throw new ExecutionException(error);
  }

  /**
   * This is an alternative way of getting the wrapped value that is more
   * convenient for the caller. It blocks uninterruptably and throws unchecked
   * exceptions.
   */
  public synchronized T sync() {
    while (!done) try {
      wait();
    } catch (InterruptedException e) {
      // Ignore it.
    } if (error == null) {
      return object;
    } if (error instanceof RuntimeException) {
      throw (RuntimeException) error;
    } throw new RuntimeException(error);
  }

  // Run then() handlers and discard any exceptions.
  private synchronized void then(T object, Throwable error) {
    if (error == null) try {
      then(object);
    } catch (Throwable t) {
      error = t;
    } if (error != null) try {
      then(error);
    } catch (Throwable t) {
      // Discard.
    }
  }

  /**
   * This is called when a {@code Bell} this {@code Bell} was promised to has
   * rung. By default, it simply rings this {@code Bell} with {@code object}.
   * Any {@code Throwable} thrown by this method will cause {@link
   * #then(Throwable)} to be called with that {@code Throwable}.
   *
   * @param object the value of the parent {@code Bell}.
   */
  protected void then(T object) throws Throwable { ring(object); }

  /**
   * This is called when a {@code Bell} this {@code Bell} was promised to has
   * failed. By default, it simply rings this {@code Bell} with {@code error}.
   * Any {@code Throwable} thrown by this method will be discarded.
   *
   * @param error the error the parent {@code Bell} was failed with.
   */
  protected void then(Throwable error) throws Throwable { ring(error); }

  /**
   * This handler is called when this {@code Bell} has rung. Subclasses can
   * override this to do something when the {@code Bell} has been rung. These
   * will be run before waiting threads are notified. Any exceptions thrown
   * will be discarded.  Implement this if you don't care about the value.
   */
  protected void done() throws Throwable { }

  /**
   * This handler is called when this {@code Bell} has rung and is passed the
   * rung value. Subclasses can override this to do something when the {@code
   * Bell} has been rung. These will be run before waiting threads are
   * notified. Any exceptions thrown will be discarded. Implement this if you
   * want to see the value.
   *
   * @param object the resolution value of this {@code Bell}.
   */
  protected void done(T object) throws Throwable { done(); }

  /**
   * This handler is called when the {@code Bell} has rung with an exception.
   * Subclasses can override this to do something when the {@code Bell} has
   * been rung.  These will be run before waiting threads are notified. Any
   * exceptions thrown will be discarded. Implement this if you don't care
   * about the error.
   */
  protected void fail() throws Throwable { }

  /**
   * This handler is called when the {@code Bell} has rung with an exception,
   * and is passed the exception. Subclasses can override this to do something
   * when the {@code Bell} has been rung. These will be run before waiting
   * threads are notified. Any exceptions thrown will be discarded. Implement
   * this if you want to see the error.
   *
   * @param error the {@code Throwable} this {@code Bell} failed with.
   */
  protected void fail(Throwable error) throws Throwable { fail(); }

  /**
   * Subclasses can override this to do something when the {@code Bell} has
   * been rung.  These will be run before waiting threads are notified. Any
   * {@code Exception}s thrown will be discarded. This will always be run after
   * either {@link #done(Object)} or {@link #fail(Throwable)}.
   */
  protected void always() throws Throwable { }

  /**
   * Promise to ring another {@code Bell} when this {@code Bell} rings.
   * Promised {@code Bell}s will ring in the order they are promised and after
   * this {@code Bell}'s handlers have been called.
   *
   * @param bell the {@code Bell} to promise to this {@code Bell}.
   * @return The value passed in for {@code bell}.
   */
  public synchronized Bell<? super T> promise(Bell<? super T> bell) {
    if (bell.isDone()) {
      return bell;  // Don't be silly...
    } while (promises == null) try {
      wait();  // We're in the process of ringing...
    } catch (InterruptedException e) {
      // Keep going...
    } if (done) {
      bell.ring(object, error);  // We've already rung, pass it on.
    } else switch (promises.size()) {
      case 0:  // This is an immutable empty list. Change to singleton.
        promises = (List) Collections.singletonList(bell);
        break;
      case 1:  // This is the singleton. Make a mutable list.
        promises = new LinkedList<Bell<? super T>>(promises);
      default:
        promises.add(bell);
    } return bell;
  }

  /**
   * Return a new {@code Bell} promised to this {@code Bell}. This can be used
   * to avoid leaking references to {@code Bell}s.
   */
  public Bell<T> detach() {
    return (Bell<T>) promise(new Bell<T>());
  }

  /**
   * A {@code Bell} which is promised to the parent {@code Bell} on
   * instantiation.
   */
  public class Promise extends Bell<T> {
    /** Create a promise which does not delegate to any other bell. */
    public Promise() { Bell.this.promise(this); }
  }

  /**
   * A {@code Bell} which is promised to the parent {@code Bell} on
   * instantiation, and performs a conversion using the {@code convert(T)}
   * method. This simplifies the {@code Bell} conversion chain pattern.
   *
   * @param <V> the supertype of objects this {@code Bell} converts.
   */
  public abstract class PromiseAs<V> extends Bell<V> {
    public PromiseAs() {
      Bell.this.new Promise() {
        public void done(T t) {
          try {
            PromiseAs.this.ring(convert(t));
          } catch (Throwable e) {
            PromiseAs.this.ring(e);
          }
        } public void fail(Throwable t) {
          try {
            PromiseAs.this.ring(convert(t));
          } catch (Throwable e) {
            PromiseAs.this.ring(e);
          }
        }
      };
    }

    /**
     * Convert the parent {@code Bell}'s ringing object into another type. Any
     * error thrown here will cause this {@code Bell} to fail with the error.
     *
     * @param t the object to convert.
     * @throws Throwable an arbitrary error.
     */
    protected abstract V convert(T t) throws Throwable;

    /**
     * Convert an error thrown the by the parent {@code Bell} into another
     * type.  Implementing this is optional, and by default will simply fail
     * this {@code Bell} with the error.
     * 
     * @param t the throwable to convert.
     * @throws Throwable an arbitrary error.
     */
    protected V convert(Throwable t) throws Throwable { throw t; }
  }

  /**
   * Return a {@code ThenAs} {@code Bell} which will ring with {@code done}
   * when this {@code Bell} rings successfully.
   *
   * @param done the value to ring the returned {@code Bell} with when this {@code
   * Bell} rings.
   */
  public final <V> ThenAs<V> thenAs(V done) {
    return new ThenAs<V>(done);
  }

  /**
   * Return a {@code ThenAs} {@code Bell} which will ring with {@code done} if
   * this {@code Bell} rings successfully and {@code fail} if this {@code Bell}
   * fails.
   *
   * @param done the value to ring the returned {@code Bell} with on success.
   * @param fail the value to ring the returned {@code Bell} with on failure.
   */
  public final <V> ThenAs<V> thenAs(V done, V fail) {
    return new ThenAs<V>(done, fail);
  }

  /**
   * A {@code Bell} which will perform some operation after the parent {@code
   * Bell} rings. This {@code Bell} is very similar to a {@link ThenAs} {@code
   * Bell}, except it has the same type as its parent, and by default its
   * {@code then(T)} method will ring the {@code Then} {@code Bell} with the
   * parent's value. If no {@code then(...)} methods are overridden, this
   * {@code Bell} behaves identically to a {@link Promise}. This class is
   * useful if the parent's value is desired on success, but another operation
   * should be performed on failure.
   */
  public class Then extends ThenAs<T> {
    public Then() { Bell.this.super(); }

    /**
     * This method will be called if the parent {@code Bell} rings successfully. By
     * default, this method rings this {@code Bell} with {@code done}.
     *
     * @param done the value the parent {@code Bell} rang with.
     */
    protected void then(T done) throws Throwable { ring(done); }
  }

  // Used in ThenAs to signal no default fail value.
  private static final Object NO_DEFAULT_FAIL = new Object();

  /**
   * A {@code Bell} which will perform some operation after the parent {@code
   * Bell} rings. When the parent {@code Bell} rings, this {@code Bell}'s
   * {@code then(...)} methods will be called depending on the result of the
   * parent {@code Bell}. By default, the {@code then(...)} methods will ring
   * this {@code Bell}, though these may be overridden to start another
   * operation which will ring this {@code Bell} instead. This can be used to
   * create an asynchronous chain of commands. If any of the {@code then(...)}
   * methods throw, this {@code Bell} is rung with the thrown object.
   *
   * @param <V> the supertype of objects this {@code Bell} rings with.
   */
  public class ThenAs<V> extends Bell<V> {
    private final V done, fail;

    /**
     * Create a {@code ThenAs} {@code Bell} which will ring with {@code null}
     * if {@code then(T)} is not overridden.
     */
    public ThenAs() { this(null, (V) NO_DEFAULT_FAIL); }

    /**
     * Create a {@code ThenAs} {@code Bell} which will ring with {@code done}
     * if {@code then(T)} is not overridden.
     *
     * @param done the default value to ring this {@code Bell} with if {@code done(T)}
     * is not overridden.
     */
    public ThenAs(V done) { this(done, (V) NO_DEFAULT_FAIL); }

    /**
     * Create a {@code ThenAs} {@code Bell} which will ring with {@code done}
     * if {@code then(T)} is not overridden, and {@code fail} if {@code
     * then(Throwable)} is not overridden.
     *
     * @param done the default value to ring this {@code Bell} with if {@code
     * done(T)} is not overridden.
     * @param fail the default value to ring this {@code Bell} with if {@code
     * done(Throwable)} is not overridden.
     */
    public ThenAs(V done, V fail) {
      this.done = done;
      this.fail = fail;
    }

    // Promise a bell to the parent which will cause this thing's then()
    // methods to run.
    {
      Bell.this.new Promise() {
        public void done(T t) {
          try { then(t); } catch (Throwable e) { ring(e); }
        } public void fail(Throwable t) {
          try { then(t); } catch (Throwable e) { ring(e); }
        } public void always() {
          try { then( ); } catch (Throwable e) { ring(e); }
        }
      };
    }

    /**
     * This method will be called if the parent {@code Bell} rings
     * successfully. By default, this method rings this {@code Bell} with
     * whatever was passed in for {@code done} in the contructor, or {@code
     * null} if the empty contructor was used.
     *
     * @param done the value the parent {@code Bell} rang with.
     */
    protected void then(T done) throws Throwable {
      ring(this.done);
    }

    /**
     * This method will be called if the parent {@code Bell} rings
     * successfully. By default, this method rings this {@code Bell} with the
     * {@code Throwable} {@code fail} passed to this method, or whatever was
     * passed in for {@code fail} in the contructor if that constructor was
     * used.
     *
     * @param fail the {@code Throwable} the parent {@code Bell} failed with.
     */
    protected void then(Throwable fail) throws Throwable {
      if (fail == NO_DEFAULT_FAIL)
        ring(fail);
      else
        ring(this.fail);
    }

    /**
     * This method will always be executed after the parent {@code Bell} rings.
     * By default, it does nothing.
     */
    protected void then() throws Throwable { }
  }

  /**
   * Set the deadline of the {@code Bell} in seconds. The deadline is relative
   * to the time this method is called. If the {@code Bell} is not rung in this
   * time, it will be resolved with a {@link TimeoutException}.
   *
   * @param deadline time in seconds after call time that the {@code Bell} may remain
   * unresolved
   */
  public synchronized Bell<T> deadline(double deadline) {
    if (!isDone()) deadlineTimerPool.schedule(new Runnable() {
      public void run() {
        ring(new TimeoutException());
      }
    }, (long)(deadline * 1E6), TimeUnit.MICROSECONDS);
    return this;
  }

  /**
   * Check if this {@code Bell} has at least one promise and all promises have
   * completed. This can be used by subclasses to implement backward-flowing
   * cancellations, for example for requests that are cancelled by its
   * requestor(s) and whose operation may thus be cancelled, or for "whichever
   * responds first" patterns.
   * <p/>
   * If this {@code Bell} is in the process of ringing or has already rung,
   * this method returns {@code false} regardless of the state of its promises
   * during its lifetime.
   *
   * @return {@code true} if there are one or more promised {@code Bell}s and they have
   * all completed; {@code false} otherwise.
   */
  public synchronized boolean promisesCompleted() {
    if (promises == null || promises.size() == 0)
      return false;
    for (Bell b : promises)
      if (!b.isDone()) return false;
    return true;
  }

  /**
   * Return a {@code Bell} which will ring with the value of this {@code Bell},
   * if this {@code Bell} rings successfully, or else will be promised to
   * {@code other} if this {@code Bell} fails.
   *
   * @param other the {@code Bell} to promise the returned {@code Bell} to if
   * this {@code Bell} fails.
   */
  public Bell<T> or(final Bell<T> other) {
    return new Then() {
      public void then(Throwable err) { other.promise(this); }
    };
  }

  /**
   * Return a {@code Bell} which will be promised to {@code other} if this
   * {@code Bell} rings successfully, or else will fail if this {@code Bell}
   * fails.
   *
   * @param other the {@code Bell} to promise the returned {@code Bell} to if
   * this {@code Bell} succeeds.
   */
  public <V super T> Bell<V> and(final Bell<V> other) {
    return new ThenAs<V>() {
      public void then(T t) { other.promise(this); }
    };
  }
}
