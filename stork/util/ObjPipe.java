package stork.util;

import java.util.*;

// ObjPipes try to tie together synchronous and asynchronous message
// passing within an application.

public class ObjPipe<T> extends Observable {
  private final ObjPipe<T> end;
  private final LinkedList<T> buffer = new LinkedList<T>();
  private ObjPipe<T> mate = null;
  private boolean isClosed = false;

  // Create a new pipe.
  public ObjPipe() {
    end = new ObjPipe<T>(this);
  }

  // Create a new pipe internally connected to another pipe.
  private ObjPipe(ObjPipe<T> other) {
    end = other;
  }

  // Get the opposite end of the pipe.
  public ObjPipe<T> end() {
    return end;
  }

  // Put an object into the pipe, allowing it to be read from the other
  // side. Return the pipe itself.
  public synchronized ObjPipe<T> put(T... ts) {
    if (isClosed)
      throw new Error("pipe is closed");
    if (mate != null)
      mate.put(ts);
    else for (T t : ts)
      end.buffer.push(t);
    setChanged();
    notifyObservers();
    return this;
  }

  // Get an object from the pipe. Returns null if there's nothing to get.
  public synchronized T get() {
    if (buffer.size() == 0 && isClosed)
      throw new Error("pipe is closed");
    return (buffer.size() > 0) ? buffer.pop() : null;
  }

  // Get the number of objects waiting to be read from the queue.
  public synchronized int count() {
    if (isClosed)
      return 0;
    return buffer.size();
  }

  // Connect this pipe to another, and propagate puts.
  public synchronized ObjPipe<T> connect(ObjPipe<T> p) {
    if (isClosed)
      throw new Error("pipe is closed");
    while (buffer.size() > 0)
      p.put(buffer.pop());
    p.connect(this);
    mate = p;
    return this;
  }

  // Returns whether or not the pipe is closed.
  public synchronized boolean isClosed() {
    return isClosed();
  }

  // Close the pipe, indicating nothing else will be written. Closings
  // propagate down the entire pipeline.
  public synchronized void close() {
    if (!isClosed) {
      isClosed = true;
      end.close();
      mate.close();
    }
  }
}
