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

  // Convenience method for creating a bell which rings when any of the given
  // bells rings successfully, or fails if they all fail.
  public static <V> Bell<V> any(Bell<V>... bells) {
    return any(Arrays.asList(bells));
  } public static <V> Bell<V> any(Collection<Bell<V>> bells) {
    final Set<Bell<V>> set = new HashSet<Bell<V>>(bells);
    final Bell<V> bell = new Bell<V>();
    if (bells.isEmpty()) {
      return bell.ring();
    } for (final Bell<V> b : bells) {
      b.promise(new Bell<V>() {
        protected void done(V v) {
          bell.ring(v);
        } protected void fail(Throwable t) {
          if (bell.isDone())
            return;
          set.remove(b);
          if (set.isEmpty())
            bell.ring(t);
        }
      });
      if (bell.isDone())
        break;
    } return bell;
  }

  // Convenience method for creating a bell which rings when all of the given
  // bells rings successfully, or fails if any fail.
  public static <V> Bell<V> all(Bell<V>... bells) {
    return all(Arrays.asList(bells));
  } public static <V> Bell<V> all(Collection<Bell<V>> bells) {
    final Set<Bell<V>> set = new HashSet<Bell<V>>(bells);
    final Bell<V> bell = new Bell<V>();
    if (bells.isEmpty()) {
      return bell.ring();
    } for (final Bell<V> b : bells) {
      b.promise(new Bell<V>() {
        protected void done(V v) {
          if (bell.isDone())
            return;
          set.remove(b);
          if (set.isEmpty())
            bell.ring(v);
        } protected void fail(Throwable t) {
          bell.ring(t);
        }
      });
      if (bell.isDone())
        break;
    } return bell;
  }

  // Convenience method for boolean bells that rings with true if any of them
  // ring with true, or false if all of them ring with false. A failed bell
  // is considered to have rung with false.
  public static Bell<Boolean> or(Bell<Boolean>... bells) {
    return or(Arrays.asList(bells));
  } public static Bell<Boolean> or(Collection<Bell<Boolean>> bells) {
    final Set<Bell<Boolean>> set = new HashSet<Bell<Boolean>>(bells);
    final Bell<Boolean> bell = new Bell<Boolean>();
    if (bells.isEmpty()) {
      return bell.ring(true);
    } for (final Bell<Boolean> b : bells) {
        b.promise(new Bell<Boolean>() {
        protected void done(Boolean v) {
          if (v)
            bell.ring(true);
        } protected void fail(Throwable t) {
          if (bell.isDone())
            return;
          set.remove(b);
          if (set.isEmpty())
            bell.ring(false);
        }
      });
      if (bell.isDone())
        break;
    } return bell;
  }

  // Convenience method for boolean bells that rings with true if all of them
  // ring with true, or false if any of them ring with false. A failed bell
  // is considered to have rung with false.
  public static Bell<Boolean> and(Bell<Boolean>... bells) {
    return and(Arrays.asList(bells));
  } public static Bell<Boolean> and(Collection<Bell<Boolean>> bells) {
    if (bells.isEmpty())
      return new Bell<Boolean>().ring(true);
    final Set<Bell<Boolean>> set = new HashSet<Bell<Boolean>>(bells);
    final Bell<Boolean> bell = new Bell<Boolean>();
      return bell.ring(true);
    } for (final Bell<Boolean> b : bells) {
      b.promise(new Bell<Boolean>() {
        protected void done(Boolean v) {
          if (bell.isDone()) {
            return;
          } if (!v) {
            bell.ring(false);
          } else {
            set.remove(b);
            if (set.isEmpty())
              bell.ring(true);
          }
        } protected void fail(Throwable t) {
          bell.ring(false);
        }
      });
      if (bell.isDone())
        break;
    } return bell;
  }

  // Helper method used by the above methods to create "multi-bells".
  private static <V> multiBell(Class<Bell<V>> c, Collection<Bell<V>> bells) {
    final Set<Bell<Boolean>> set = new HashSet<Bell<Boolean>>(bells);
    final Bell<Boolean> bell = new Bell<Boolean>();
    if (bells.isEmpty()) {
      return bell.ring(true);
    } for (final Bell<Boolean> b : bells) {
      b.promise(new Bell<Boolean>() {
        protected void done(Boolean v) {
          if (bell.isDone()) {
            return;
          } if (!v) {
            bell.ring(false);
          } else {
            set.remove(b);
            if (set.isEmpty())
              bell.ring(true);
          }
        } protected void fail(Throwable t) {
          bell.ring(false);
        }
      });
      if (bell.isDone())
        break;
    } return bell;
  }
}
