package stork.feather;

import java.util.*;

/**
 * An asynchronous analogue to an {@code Iterator} based on {@code Bell}s. This
 * can be used, for instance, to implement an asynchronous producer-consumer
 * solution.
 */
public class Emitter<T> extends Bell {
  // Bells which are awaiting pairing.
  private final Queue<Bell<T>> queue = new LinkedList<Bell<T>>();
  private boolean queueHasEmits = false;

  // When done, finalize and clear the queue.
  {
    new Promise() {
      public void done()            { finalizeRemaining(null); }
      public void fail(Throwable t) { finalizeRemaining(t); }
    };
  }

  public abstract class ForEach extends Bell {
    { pull(); }
    private void pull() {
      Emitter.this.get(new Bell<T>() {
        public void then(T t) throws Throwable {
          each(t);
          ring();
        } public void done() {
          pull();
        } public void fail(Throwable t) {
          if (isCancelled())
            ForEach.this.ring();
          else
            ForEach.this.ring(t);
        }
      });
    }
    public abstract void each(T t) throws Throwable;
  }

  /** Emit {@code null}. */
  public final synchronized void emit() {
    emit((T) null);
  }

  /** Emit an item. */
  public final synchronized void emit(T item) {
    emit(item == null ? (Bell<T>)Bell.rungBell() : new Bell<T>(item));
  }

  /** Emit an item via a {@code Bell}. */
  public final synchronized void emit(Bell<T> bell) {
    if (bell == null) {
      bell = (Bell<T>) Bell.rungBell();
    } if (isDone()) {
      bell.cancel();
    } else if (queue.isEmpty() || queueHasEmits) {
      queue.add(bell);
      queueHasEmits = true;
    } else {
      bell.promise(queue.poll());
    }
  }

  /** Emit all items in an array. */
  public final synchronized void emitAll(T[] items) {
    if (!isDone()) for (T t : items) emit(t);
  }

  /** Get a {@code Bell} which will ring with the next item. */
  public final synchronized Bell<T> get() {
    return get(null);
  }

  /** Ring {@code bell} with the next item. */
  public final synchronized Bell<T> get(Bell<T> bell) {
    if (bell == null) {
      bell = new Bell<T>();
    } else if (!queue.isEmpty() && queueHasEmits) {
      queue.poll().promise(bell);
    } else if (isDone()) {
      bell.cancel();
    } else {
      queue.add(bell);
      queueHasEmits = false;
    } return bell;
  }

  // Upon ringing, cancel any queued bells.
  private final synchronized void finalizeRemaining(Throwable error) {
    if (queue == null)
      return;
    if (error != null && !queueHasEmits)
      for (Bell<T> b : queue) b.ring(error);
    else if (!queueHasEmits)
      for (Bell<T> b : queue) b.cancel();
  }

  protected void finalize() {
    finalizeRemaining(null);
  }

  /** Create an emitter from an array. */
  public static <V> Emitter<V> from(final V[] v) {
    Emitter<V> emitter = new Emitter<V>();
    emitter.emitAll(v);
    return emitter;
  }

  /** Create an emitter from an array {@code Bell}. */
  public static <V> Emitter<V> from(final Bell<V[]> bell) {
    final Emitter<V> emitter = new Emitter<V>();
    bell.new Promise() {
      public void done(V[] vs) {
        emitter.emitAll(vs);
        emitter.ring();
      } public void fail(Throwable t) {
        emitter.ring(t);
      }
    };
    return emitter;
  }
}
