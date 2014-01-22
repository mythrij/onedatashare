package stork.util;

import java.util.*;
import java.util.concurrent.*;

// A minimalistic promise primitive used for synchronization. It can be
// extended to implement callbacks to execute upon resolution.

public class Bell<T> implements Future<T> {
  private T object;
  private Throwable error;
  private boolean done = false;
  private Set<Bell<? super T>> promises = Collections.emptySet();

  // Ring the bell with the given object and wake up any waiting threads.
  public final Bell<T> ring(T object) {
    return ring(object, null);
  }

  // Ring the bell with null and wake up any waiting threads.
  public final Bell<T> ring() {
    return ring(null, null);
  }

  // Ring the bell with the given error and wake up any waiting threads.
  public final Bell<T> ring(Throwable error) {
    return ring(null, (error != null) ? error : new NullPointerException());
  }

  // Used by the other ring methods.
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

  // Cancel the bell, resolving it with a cancellation error. Returns true if
  // the bell was cancelled as a result of this call, false otherwise.
  public synchronized boolean cancel(boolean mayInterruptIfRunning) {
    if (done)
      return false;
    ring(null, new CancellationException());
    return true;
  }

  // Return true if this bell was rung with a cancellation exception.
  public synchronized boolean isCancelled() {
    if (!done)
      return false;
    return error != null && error instanceof CancellationException;
  }

  // Return true if the bell has been rung.
  public synchronized boolean isDone() {
    return done;
  }

  // Return true if the bell rang successfully.
  public synchronized boolean isSuccessful() {
    return done && error == null;
  }

  // Return true if the bell failed.
  public synchronized boolean isFailed() {
    return done && error != null;
  }

  // Wait for the bell to be rung, then return the value.
  public synchronized T get()
  throws InterruptedException, ExecutionException {
    while (!done)
      wait();
    return getOrThrow();
  }

  // Wait for the bell to be rung up to the specified time, then return the
  // value.
  public synchronized T get(long timeout, TimeUnit unit)
  throws InterruptedException, ExecutionException, TimeoutException {
    if (!done)
      unit.timedWait(this, timeout);
    if (!done)
      throw new TimeoutException();
    return getOrThrow();
  }

  // Either get the object or throw the wrapped error. Only call if done.
  private T getOrThrow() throws ExecutionException {
    if (error == null)
      return object;
    if (error instanceof CancellationException)
      throw (CancellationException) error;
    if (error instanceof ExecutionException)
      throw (ExecutionException) error;
    throw new ExecutionException(error);
  }

  // This is an alternative way of getting the wrapped value that is more
  // convenient for the caller. It blocks uninterruptably and throws unchecked
  // exceptions.
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

  // Subclasses can override these to do something when the bell has been rung.
  // These will be run before waiting threads are notified. Any exceptions
  // thrown will be discarded.
  protected void done() throws Throwable {
    // Implement this if you don't care about the value.
  } protected void done(T object) throws Throwable {
    // Implement this if you want to see the value.
    done();
  } protected void fail() throws Throwable {
    // Implement this if you don't care about the error.
  } protected void fail(Throwable error) throws Throwable {
    // Implement this if you want to see the error.
    fail();
  } protected void always() throws Throwable {
    // This will always be run after either done() or fail().
  }

  // Promise to ring another bell when this bell rings. The order promised
  // bells will be rung is not specified. However, promised bells are
  // guaranteed to be rung after this bell's handlers have been called.
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

  // Utility Methods
  // ===============
  // Used by multi-bells to implement behavior regarding how to handle bells as
  // they ring. The one bell passed to the handlers will always be rung.
  private static abstract class Aggregator<I,O> {
    // Check a value/error, ring the bell if so desired.
    protected void done(Bell<I> one, Bell<O> all) { }
    protected void fail(Bell<I> one, Bell<O> all) { }
    protected void always(Bell<I> one, Bell<O> all) { }
    // The default result if the bell is not rung by the above.
    protected void end(Bell<I> last, Bell<O> all) { }
  }

  // Helper method used by the above methods to create "multi-bells". The
  // returned bell will be rung whenever, as a result of a pass bell ringing,
  // the aggregator causes the returned bell to ring. If all the passed bells
  // ring and the aggregator hasn't rung the returned bell, the aggregator's
  // end method will be called, which is expected to ring the bell. If it does
  // not, the returned bell will be rung with null.
  private static <I,O> Bell<O> multiBell(
      Collection<Bell<I>> bells, final Aggregator<I,O> agg) {
    final Set<Bell<I>> set = new HashSet<Bell<I>>(bells);
    final Bell<O> bell = new Bell<O>();
    if (bells.isEmpty()) {
      agg.end(null, bell);
      if (!bell.isDone())
        bell.ring();
      return bell;
    } for (final Bell<I> b : bells) {
      b.promise(new Bell<I>() {
        protected void always() {
          synchronized (bell) {
            if (bell.isDone()) return;

            // Check with the aggregator.
            if (b.isSuccessful())
              agg.done(b, bell);
            else if (b.isFailed())
              agg.fail(b, bell);
            agg.always(b, bell);

            // Remove from the set. If done, run end().
            if (bell.isDone()) return;
            set.remove(b);
            if (set.isEmpty()) {
              agg.end(b, bell);
              if (!bell.isDone()) bell.ring();
            }
          }
        }
      });
      if (bell.isDone())
        break;
    } return bell;
  }

  // Convenience method for creating a bell which rings when any of the given
  // bells rings successfully, or fails if they all fail.
  public static <V> Bell<Bell<V>> any(Bell<V>... bells) {
    return any(Arrays.asList(bells));
  } public static <V> Bell<Bell<V>> any(Collection<Bell<V>> bells) {
    return multiBell(bells, new Aggregator<V,Bell<V>>() {
      protected void done(Bell<V> one, Bell<Bell<V>> all) {
        all.ring(one);
      } protected void end(Bell<V> one, Bell<Bell<V>> all) {
        all.ring(one.error);
      }
    });
  }

  // Convenience method for creating a bell which rings when all of the given
  // bells rings successfully, or fails if any fail.
  public static <V> Bell<Bell<V>> all(Bell<V>... bells) {
    return all(Arrays.asList(bells));
  } public static <V> Bell<Bell<V>> all(Collection<Bell<V>> bells) {
    return multiBell(bells, new Aggregator<V,Bell<V>>() {
      protected void fail(Bell<V> one, Bell<Bell<V>> all) {
        all.ring(one.error);
      } protected void end(Bell<V> one, Bell<Bell<V>> all) {
        all.ring(one);
      }
    });
  }

  // Convenience method for boolean bells that rings with true if any of them
  // ring with true, or false if all of them ring with false. A failed bell
  // is considered to have rung with false.
  public static Bell<Boolean> or(Bell<Boolean>... bells) {
    return or(Arrays.asList(bells));
  } public static Bell<Boolean> or(Collection<Bell<Boolean>> bells) {
    return multiBell(bells, new Aggregator<Boolean,Boolean>() {
      protected void done(Bell<Boolean> one, Bell<Boolean> all) {
        if (one.sync())
          all.ring(true);
      } protected void end(Bell<Boolean> one, Bell<Boolean> all) {
        all.ring(one == null);
      }
    });
  }

  // Convenience method for boolean bells that rings with true if all of them
  // ring with true, or false if any of them ring with false. A failed bell
  // is considered to have rung with false.
  public static Bell<Boolean> and(Bell<Boolean>... bells) {
    return and(Arrays.asList(bells));
  } public static Bell<Boolean> and(Collection<Bell<Boolean>> bells) {
    return multiBell(bells, new Aggregator<Boolean,Boolean>() {
      protected void fail(Bell<Boolean> one, Bell<Boolean> all) {
        all.ring(false);
      } protected void end(Bell<Boolean> one, Bell<Boolean> all) {
        all.ring(one == null);
      }
    });
  }
}
