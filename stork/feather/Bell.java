package stork.feather;

import java.util.*;
import java.util.concurrent.*;

/**
 * A promise primitive used for stringing together the results of asynchronous
 * operations and executing asynchronous handlers. It supports callbacks
 * defined in subclasses, chaining of results, deadlines, and
 * back-cancellation.
 *
 * @param <T> the supertype of objects that can ring this bell.
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
   * Ring the bell with the given object and wake up any waiting threads.
   *
   * @param object The object to ring the bell with.
   * @return This bell.
   */
  public final Bell<T> ring(T object) {
    return ring(object, null);
  }

  /**
   * Ring the bell with null and wake up any waiting threads.
   *
   * @return This bell.
   */
  public final Bell<T> ring() {
    return ring(null, null);
  }

  /**
   * Ring the bell with the given error and wake up any waiting threads.
   *
   * @param error The error to ring the bell with.
   * @return This bell.
   */
  public final Bell<T> ring(Throwable error) {
    return ring(null, (error != null) ? error : new NullPointerException());
  }

  /**
   * Used by the other ring methods. If error is not null, assume failure.
   *
   * @param object The object to ring the bell with.
   * @param error The error to ring the bell with.
   * @return This bell.
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
      b.ring(object, error);
    promises = Collections.emptyList();

    synchronized (this) {
      notifyAll();
    }

    return this;
  }

  /**
   * Cancel the bell, resolving it with a cancellation error. This is here to
   * satisfy the requirements of the {@code Future} interface.
   *
   * @param mayInterruptIfRunning Ignored in this implementation.
   * @return {@code true} if the bell was cancelled as a result of this call,
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
   * Cancel the bell, resolving it with a cancellation error. This returns a
   * reference to this bell, unlike {@link #cancel(boolean)}.
   *
   * @return This {@code Bell}.
   */
  public Bell<T> cancel() {
    cancel(true);
    return this;
  }

  /**
   * @return {@code true} if this bell was rung with a cancellation exception.
   * @see Future#isCancelled
   */
  public synchronized boolean isCancelled() {
    if (!done)
      return false;
    return error != null && error instanceof CancellationException;
  }

  /** Return true if the bell has been rung. */
  public synchronized boolean isDone() {
    return done;
  }

  /** Return true if the bell rang successfully. */
  public synchronized boolean isSuccessful() {
    return done && error == null;
  }

  /** Return true if the bell failed. */
  public synchronized boolean isFailed() {
    return done && error != null;
  }

  /** Wait for the bell to be rung, then return the value. */
  public synchronized T get()
  throws InterruptedException, ExecutionException {
    while (!done)
      wait();
    return getOrThrow();
  }

  /**
   * Wait for the bell to be rung up to the specified time, then return the
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

  /**
   * This handler is called when the bell has rung. Subclasses can override
   * this to do something when the bell has been rung. These will be run before
   * waiting threads are notified. Any exceptions thrown will be discarded.
   * Implement this if you don't care about the value.
   */
  protected void done() throws Throwable { }

  /**
   * This handler is called when the bell has rung and is passed the rung
   * value. Subclasses can override this to do something when the bell has been
   * rung. These will be run before waiting threads are notified. Any
   * exceptions thrown will be discarded. Implement this if you want to see the
   * value.
   */
  protected void done(T object) throws Throwable { done(); }

  /**
   * This handler is called when the bell has rung with an exception.
   * Subclasses can override this to do something when the bell has been rung.
   * These will be run before waiting threads are notified. Any exceptions
   * thrown will be discarded. Implement this if you don't care about the
   * error.
   */
  protected void fail() throws Throwable { }

  /**
   * This handler is called when the bell has rung with an exception, and is
   * passed the exception. Subclasses can override this to do something
   * when the bell has been rung. These will be run before waiting threads are
   * notified. Any exceptions thrown will be discarded. Implement this if you
   * want to see the error.
   */
  protected void fail(Throwable error) throws Throwable { fail(); }

  /**
   * Subclasses can override this to do something when the bell has been rung.
   * These will be run before waiting threads are notified. Any exceptions
   * thrown will be discarded. This will always be run after either {@link
   * #done(Object)} or {@link #fail(Throwable)}.
   */
  protected void always() throws Throwable { }

  /**
   * Promise to ring another bell when this bell rings. Promised bells will
   * ring in the order they are promised and after this bell's handlers have
   * been called.
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
   * A bell which is promised to the parent bell on instantiation.
   */
  public class Promise extends Bell<T> {
    /** Create a promise which does not delegate to any other bell. */
    public Promise() { Bell.this.promise(this); }
  }

  /**
   * A bell which is promised to the parent bell on instantiation, and performs
   * a conversion using the {@code convert(T)} method. This
   * simplifies the bell conversion chain pattern.
   *
   * @param <V> the supertype of objects this bell converts.
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
     * Convert the parent bell's ringing object into another type. Any error
     * thrown here will cause this bell to fail with the error.
     *
     * @param t the object to convert.
     * @throws Throwable an arbitrary error.
     */
    protected abstract V convert(T t) throws Throwable;

    /**
     * Convert an error thrown the by the parent bell into another type.
     * Implementing this is optional, and by default will simply fail this bell
     * with the error.
     * 
     * @param t the throwable to convert.
     * @throws Throwable an arbitrary error.
     */
    protected V convert(Throwable t) throws Throwable { throw t; }
  }

  /**
   * Return a {@code ThenAs} bell which will ring with {@code done} when this
   * bell rings successfully.
   *
   * @param done the value to ring the returned bell with when this {@code
   * Bell} rings.
   */
  public final <V> ThenAs<V> thenAs(V done) {
    return new ThenAs<V>(done);
  }

  /**
   * Return a {@code ThenAs} bell which will ring with {@code done} if this
   * bell rings successfully and {@code fail} if this bell fails.
   *
   * @param done the value to ring the returned bell with on success.
   * @param fail the value to ring the returned bell with on failure.
   */
  public final <V> ThenAs<V> thenAs(V done, V fail) {
    return new ThenAs<V>(done, fail);
  }

  /**
   * A bell which will perform some operation after the parent bell rings. This
   * bell is very similar to a {@link ThenAs} bell, except it has the same type
   * as its parent, and by default its {@code then(T)} method will ring the
   * {@code Then} bell with the parent's value. If no {@code then(...)} methods
   * are overridden, this bell behaves identically to a {@link Promise}. This
   * class is useful if the parent's value is desired on success, but another
   * operation should be performed on failure.
   */
  public class Then extends ThenAs<T> {
    public Then() { Bell.this.super(); }

    /**
     * This method will be called if the parent bell rings successfully. By
     * default, this method rings this bell with {@code done}.
     *
     * @param done the value the parent bell rang with.
     */
    protected void then(T done) throws Throwable { ring(done); }
  }

  // Used in ThenAs to signal no default fail value.
  private static final Object NO_DEFAULT_FAIL = new Object();

  /**
   * A bell which will perform some operation after the parent bell rings. When
   * the parent bell rings, this bell's {@code then(...)} methods will be
   * called depending on the result of the parent bell. By default, the {@code
   * then(...)} methods will ring this bell, though these may be overridden to
   * start another operation which will ring this bell instead. This can be
   * used to create an asynchronous chain of commands. If any of the {@code
   * then(...)} methods throw, this {@code Bell} is rung with the thrown
   * object.
   *
   * @param <V> the supertype of objects this bell rings with.
   */
  public class ThenAs<V> extends Bell<V> {
    private final V done, fail;

    /**
     * Create a {@code ThenAs} bell which will ring with {@code null} if {@code
     * then(T)} is not overridden.
     */
    public ThenAs() { this(null, (V) NO_DEFAULT_FAIL); }

    /**
     * Create a {@code ThenAs} bell which will ring with {@code done} if {@code
     * then(T)} is not overridden.
     *
     * @param done the default value to ring this bell with if {@code done(T)}
     * is not overridden.
     */
    public ThenAs(V done) { this(done, (V) NO_DEFAULT_FAIL); }

    /**
     * Create a {@code ThenAs} bell which will ring with {@code done} if {@code
     * then(T)} is not overridden, and {@code fail} if {@code then(Throwable)}
     * is not overridden.
     *
     * @param done the default value to ring this bell with if {@code done(T)}
     * is not overridden.
     * @param fail the default value to ring this bell with if {@code
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
     * This method will be called if the parent bell rings successfully. By
     * default, this method rings this bell with whatever was passed in for
     * {@code done} in the contructor, or {@code null} if the empty contructor
     * was used.
     *
     * @param done the value the parent bell rang with.
     */
    protected void then(T done) throws Throwable {
      ring(this.done);
    }

    /**
     * This method will be called if the parent bell rings successfully. By
     * default, this method rings this bell with the {@code Throwable} {@code
     * fail} passed to this method, or whatever was passed in for {@code fail}
     * in the contructor if that constructor was used.
     *
     * @param fail the {@code Throwable} the parent bell failed with.
     */
    protected void then(Throwable fail) throws Throwable {
      if (fail == NO_DEFAULT_FAIL)
        ring(fail);
      else
        ring(this.fail);
    }

    /**
     * This method will always be executed after the parent bell rings. By
     * default, it does nothing.
     */
    protected void then() throws Throwable { }
  }

  /**
   * Set the deadline of the bell in seconds. The deadline is relative to the
   * time this method is called. If the bell is not rung in this time, it will
   * be resolved with a {@link TimeoutException}.
   *
   * @param deadline time in seconds after call time that the bell may remain
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
   * Check if this bell has at least one promise and all promises have
   * completed. This can be used by subclasses to implement backward-flowing
   * cancellations, for example for requests that are cancelled by its
   * requestor(s) and whose operation may thus be cancelled, or for "whichever
   * responds first" patterns.
   * <p/>
   * If this bell is in the process of ringing or has already rung, this method
   * returns {@code false} regardless of the state of its promises during its
   * lifetime.
   *
   * @return {@code true} if there are one or more promised bells and they have
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
  public Bell<T> and(final Bell<T> other) {
    return new Then() {
      public void then(T t) { other.promise(this); }
    };
  }

  /**
   * Base class for bells whose resolution value depends on the resolution
   * values of a collection of other bells. Every time a wrapped bell rings,
   * the method {@code check(Bell)} is called, which should return {@code true}
   * if the passed bell should ring the multi-bell. If all the passed bells
   * have rung and {@code check(Bell)} hasn't rung this bell, {@code end(Bell)}
   * will be called, and is passed the last bell to ring.  If it does not ring
   * the bell, this bell will be rung with {@code null}.
   *
   * The default implementation rings with {@code null} when all of the wrapped
   * bells have rung.
   *
   * @param <I> the type of this {@code MultiBell}'s input bells
   */
  public static class MultiBell<I> extends Bell<I> {
    private final Set<Bell<I>> set;

    /**
     * Create a multi-bell which will ring when either all of {@code bells} have
     * rung or {@link #check(Bell)} causes this bell to ring.
     *
     * @param bells the bells to aggegate
     */
    protected MultiBell(Bell<I>... bells) {
      this(Arrays.asList(bells));
    }

    /**
     * Create a multi-bell which will ring when either all of {@code bells} have
     * rung or {@link #check(Bell)} causes this bell to ring.
     *
     * @param bells the bells to aggegate
     */
    protected MultiBell(Collection<Bell<I>> bells) {
      set = new HashSet<Bell<I>>(bells);

      if (bells.isEmpty()) {
        // Degenerate case...
        nonThrowingCheck(null);
        if (!isDone()) ring();
      } else for (final Bell<I> b : bells) {
        b.new Promise() {
          protected void always() {
            set.remove(b);
            synchronized (MultiBell.this) {
              if (!MultiBell.this.isDone()) 
                nonThrowingCheck(b);
              if (!MultiBell.this.isDone() && isLast())
                MultiBell.this.ring();
            }
          }
        };
        if (isDone()) break;  // See if we can stop early.
      }
    }

    private void nonThrowingCheck(Bell<I> one) {
      try {
        check(one);
      } catch (Throwable t) { }
    }

    /**
     * A method which is called every time a wrapped bell rings, and may ring
     * this bell. Any exception this method throws is ignored.
     *
     * @param one a bell which has just rung, or {@code null} if the wrapped
     * set is empty
     * @throws Throwable arbitrary exception; should be ignored by caller
     */
    protected void check(Bell<I> one) throws Throwable { }

    /**
     * Used inside {@code check(Bell)} to determine if all the bells have
     * rung.
     *
     * @return {@code true} if all the bells have rung; {@code false} otherwise
     */
    protected final synchronized boolean isLast() {
      return set.isEmpty();
    }
  }

  /**
   * A bell which rings when the first of the given bells rings successfully,
   * or else fails if they all fail. The value of this bell is the value of the
   * first bell which rang successfully.
   */
  public static class Any<V> extends MultiBell<V> {
    public Any(Bell<V>... bells) { super(bells); }
    public Any(Collection<Bell<V>> bells) { super(bells); }
    /** Ring when the first bell rings successfully. */
    protected void check(Bell<V> one) {
      if (one != null && (one.isSuccessful() || isLast()))
        one.promise(this);
    }
  }

  /**
   * A bell which rings when all of the given bells rings successfully, or
   * fails if any fail. The value of this bell is the value of the last bell
   * which rang successfully.
   */
  public static class All<V> extends MultiBell<V> {
    public All(Bell<V>... bells) { super(bells); }
    public All(Collection<Bell<V>> bells) { super(bells); }
    /** Fail if any bell fails. */
    protected void check(Bell<V> one) {
      if (one != null && (one.isFailed() || isLast()))
        one.promise(this);
    }
  }

  /**
   * A bell which rings with true if any of the wrapped bells ring with true,
   * or false if all of them ring with false. A failed bell is considered to
   * have rung with false.
   */
  public static class Or extends MultiBell<Boolean> {
    public Or(Bell<Boolean>... bells) { super(bells); }
    public Or(Collection<Bell<Boolean>> bells) { super(bells); }
    /** If true, ring with true. */
    protected void check(Bell<Boolean> one) {
      if (one == null || one.object)
        ring(true);
    }
  }

  /**
   * A bell which rings with {@code true} if all of the wrapped bells ring with
   * {@code true}, or false if any of them ring with {@code false}. A failed
   * bell is considered to have rung with {@code false}.
   */
  public static class And extends MultiBell<Boolean> {
    public And(Bell<Boolean>... bells) { super(bells); }
    public And(Collection<Bell<Boolean>> bells) { super(bells); }
    /** If true, ring with true. */
    protected void check(Bell<Boolean> one) {
      if (one == null)
        ring(true);
      if (one.isFailed() || !one.object)
        ring(false);
    }
  }
}
