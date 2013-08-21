package stork.util;

import java.util.*;
import java.util.concurrent.*;

public class Pipe<T> {
  Set<End> ends = new HashSet<End>();
  private boolean closed = false;

  public class End {
    BlockingQueue<T> queue;
    boolean closed = false;

    // Add this end to the pipe's end set.
    public End() {
      this(true);
    } public End(boolean buffered) {
      synchronized (Pipe.this) {
        ends.add(this);
      } queue = buffered ? new LinkedBlockingQueue<T>() : null;
    }

    // Put messages from this end into the pipe.
    @SuppressWarnings({"unchecked"})
    public final void put(T... msgs) {
      for (T t : msgs) if (t != null)
        broadcast(this, t);
    }
      
    // Get a message from this end if there is one.
    public T get() {
      return get(true);
    } public T get(boolean block) {
      if (queue == null) {
        return null;
      } try {
        return (block && !closed) ? queue.take() : queue.poll();
      } catch (Exception e) {
        return null;
      }
    }

    // Close the pipe end. All further get()s will return null.
    public synchronized void close() {
      closed = true;
    }

    // This is called when a message is broadcast into the pipe.
    private void store(End sender, T msg) {
      store(msg);
    } public void store(T msg) {
      if (queue != null) try {
        queue.put(msg);
      } catch (Exception e) {
        // Eh...
      }
    }
  }

  // Send messages to everything in the pipe other than the sender,
  // unless there's only one pipe end, in which case the pipe end will
  // receive its own message. (This makes simple pipes easier.)
  private synchronized void broadcast(End from, T msg) {
    if (closed) {
      return;
    } try {
      if (ends.size() == 1) for (End e : ends) {
        if (!e.closed) e.store(from, msg);
      } else for (End e : ends) {
        if (e != from && !e.closed) e.store(from, msg);
      }
    } catch (Exception ex) {
      // Don't worry about it...
    }
  }

  // Close all pipe ends and prevent further broadcasts.
  public synchronized void close() {
    if (!closed) for (End e : ends)
      e.close();
    closed = true;
  }
}
