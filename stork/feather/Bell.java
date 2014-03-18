package stork.feather;

import java.util.*;
import java.util.concurrent.*;

/**
 * A promise primitive used for stringing together the results of asynchronous
 * operations and executing asynchronous handlers. It supports callbacks
 * defined in subclasses, chaining of results, and deadlines.
 */
public class Bell<T> implements Future<T> {
  private T object;
  private Throwable error;
  private boolean done = false;
  private Set<Bell<? super T>> promises = Collections.emptySet();

  // This is used to schedule bell deadlines.
  private static ScheduledExecutorService deadlineTimerPool =
    new ScheduledThreadPoolExecutor(1);

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
    Set<Bell<? super T>> proms;

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

    // Pass along to all the promises, then put an empty set back.
    for (Bell<? super T> b : proms)
      b.ring(object, error);
    promises = Collections.emptySet();

    synchronized (this) {
      notifyAll();
    }

    return this;
  }

  /**
   * Cancel the bell, resolving it with a cancellation error.
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
   * Promise to ring another bell when this bell rings. The order promised
   * bells will be rung is not specified. However, promised bells are
   * guaranteed to be rung after this bell's handlers have been called.
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
    } else if (!promises.contains(bell)) switch (promises.size()) {
      case 0:  // This is an immutable empty set. Change to singleton.
        promises = (Set<Bell<? super T>>)(Object) Collections.singleton(bell);
        break;
      case 1:  // This is the singleton. Make a mutable set.
        promises = new HashSet<Bell<? super T>>(promises);
      default:
        promises.add(bell);
    } return bell;
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
   * Base class for bells whose resolution value depends on the resolution
   * values of a collection of other bells. Every time a wrapped bell rings,
   * the method {@link MultiBell#check(Bell)} is called, which should return
   * {@code true} if the passed bell should ring the multi-bell. If all the
   * passed bells have rung and {@link MultiBell#check(Bell)} hasn't rung this
   * bell, {@link MultiBell#end(Bell)} will be called, and is passed the last
   * bell to ring.  If it does not ring the bell, this bell will be rung with
   * {@code null}.
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
        b.promise(new Bell<I>() {
          protected void always() {
            set.remove(b);
            synchronized (MultiBell.this) {
              if (!MultiBell.this.isDone()) 
                nonThrowingCheck(b);
              if (!MultiBell.this.isDone() && isLast())
                MultiBell.this.ring();
            }
          }
        });
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
     * Used inside {@link #check(boolean)} to determine if all the bells have
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
