package stork.util;

import java.util.*;
import java.util.concurrent.*;

// A minimalistic promise primitive used for synchronization. It can be
// extended to implement callbacks to execute upon resolution.

public class Bell<T> implements Future<T> {
  private T object;
  private Throwable error;
  private boolean done = false;

  // Ring the bell with the given object and wake up any waiting threads.
  public final synchronized Bell<T> ring(T object) {
    return ring(object, null);
  }

  // Ring the bell with null and wake up any waiting threads.
  public final synchronized Bell<T> ring() {
    return ring(null, null);
  }

  // Ring the bell with the given error and wake up any waiting threads.
  public final synchronized Bell<T> ring(Throwable error) {
    return ring(null, (error != null) ? error : new NullPointerException());
  }

  // Used by the other ring methods. Only call from a synchronized context.
  private Bell<T> ring(T object, Throwable error) {
    if (!done) {
      done = true;
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
      } finally {
        notifyAll();
      }
    } return this;
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
}
