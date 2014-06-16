package stork.feather;

import java.util.*;

/**
 * An asynchronous analogue to an {@code Iterator} based on {@code Bell}s. This
 * can be used, for instance, to implement an asynchronous producer-consumer
 * solution.
 */
public class Emitter<T> extends Bell {
  // Bells which are awaiting pairing.
  private Queue<Bell<T>> queue = new LinkedList<Bell<T>>();
  private boolean queueHasEmits = false;

  // When done, finalize and clear the queue.
  {
    new Promise() {
      public void done()            { finalizeRemaining(null); }
      public void fail(Throwable t) { finalizeRemaining(t); }
    };
  }

  /** Emit {@code null}. */
  public final synchronized void emit() {
    emit((T) null);
  }

  /** Emit an item. */
  public final synchronized void emit(T item) {
    emit(item == null ? (Bell<T>) Bell.rungBell() : new Bell<T>(item));
  }

  /** Emit an item via a {@code Bell}. */
  public final synchronized void emit(Bell<T> bell) {
    if (bell == null) {
      bell = (Bell<T>) Bell.rungBell();
    } if (isDone()) {
      bell.cancel();
    } if (queue.isEmpty() || !queueHasEmits) {
      queue.add(bell);
      queueHasEmits = false;
    } else {
      bell.promise(queue.poll());
    }
  }

  /** Get a {@code Bell} which will ring with the next item. */
  public final synchronized Bell<T> get() {
    return get(null);
  }

  /** Ring {@code bell} with the next item. */
  public final synchronized Bell<T> get(Bell<T> bell) {
    if (bell == null) {
      bell = new Bell<T>();
    } if (isDone()) {
      promise(bell);
    } else if (queue.isEmpty() || queueHasEmits) {
      queue.add(bell);
      queueHasEmits = true;
    } else {
      queue.poll().promise(bell);
    } return bell;
  }

  // Upon ringing, cancel any queued bells.
  private final synchronized void finalizeRemaining(Throwable error) {
    if (queue == null)
      return;
    if (error != null && !queueHasEmits) for (Bell<T> b : queue)
      b.ring(error);
    else for (Bell<T> b : queue)
      b.cancel();
    queue = null;
  }
}
