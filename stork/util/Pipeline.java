package stork.util;

import java.util.*;

// A pipeline can be used by command-based protocols to send multiple commands
// across a channel without waiting for acknowledgment. The pipelining level
// specifies the number of unacknowledged commands that can be in the pipeline
// before more are allowed to be sent. If the pipeline is full, the pipe()
// method will block. The pipelining level can also be set to zero to do
// "infinite" pipelining.
//
// A pipelined command consists of a send handler, a receive handler, and an
// abort handler. The send handler actually handles issuing the command.  The
// receive handler is called when it is the command's turn to have its
// acknowledgement reply handled. The abort handler allows the command to be
// canceled (if supported).

public abstract class Pipeline<C,R> extends Thread {
  private final LinkedList<PipeCommand> sent, deferred;
  private int pending = 0;
  private int level;
  private boolean dead = false;

  public Pipeline(int l) {
    super("pipeline");
    sent = new LinkedList<PipeCommand>();
    deferred = new LinkedList<PipeCommand>();
    setPipelining(l);
    setDaemon(true);
    start();
  } public Pipeline() {
    this(0);
  }

  // Internal representation of a command passed to the pipeliner.
  private class PipeCommand extends Bell<R> {
    C cmd;

    PipeCommand(C c) {
      cmd = c;
    }

    // Read a reply and try to ring the bell with it. Returns true if the bell
    // rang, false otherwise. If the pipeline was interrupted while waiting for the reply,
    // throws an interrupted exception.
    boolean handle() throws InterruptedException {
      if (cmd == null) {
        return ring().rang();
      } try {
        return ring(handleReply()).rang();
      } catch (InterruptedException t) {
        throw t;
      } catch (Throwable t) {
        // Otherwise, it was some other exception.
        return ring(t).rang();
      }
    }
  }

  // Write a command to the queue to be piped.
  public synchronized Bell pipe(C c) {
    return pipe(new PipeCommand(c));
  } private synchronized Bell pipe(PipeCommand c) {
    deferred.add(c);
    notifyAll();
    return c;
  }

  // Return a synchronization future. I.e., a future that will be resolved once
  // the pipeline has reached the point where sync() was called.
  public synchronized Bell sync() {
    return pipe(null);
  }

  // Subclasses should implement write behavior. Should return true if the
  // command was sent, or false to indicate we should call this again later. It
  // should only throw if the connection is closed.
  protected abstract boolean handleWrite(C c);

  // Subclasses should implement read behavior. This should block until a reply
  // is received.
  protected abstract R handleReply(C c) throws InterruptedException;

  // Set the number of commands that are allowed to be pending (i.e., waiting
  // for replies). A value less than 1 means an unlimited number of commands
  // are allowed to be pending.
  public synchronized void setPipelining(int n) {
    level = (n < 0) ? 0 : n;
  }

  // Reads replies from sent commands with the read handler then adds them to
  // the done queue.
  public void run() {
    while (!dead) try {
      waitForWork();
      writeDeferred();

      synchronized (sent) {
        if (sent.isEmpty())
          continue;
        PipeCommand c = sent.peek();
        if (c.handle())
          sent.pop();
        if (c.cmd != null)
          pending--;
      }
    } catch (InterruptedException e) {
      for (PipeCommand p : sent)     p.cancel();
      for (PipeCommand p : deferred) p.cancel();
      dead = true;
      notifyAll();
    }
  }

  // Block until something has been deferred or sent.
  private synchronized void waitForWork() throws InterruptedException {
    while (sent.isEmpty() && deferred.isEmpty()) wait();
  }

  // Write a command and update the pending level.
  private synchronized boolean tryWrite(PipeCommand c) {
    try {
      if (!c.bell.ready()) return false;
    } catch (Throwable t) {
      c.bell.ring(null, t);
      return true;
    } if (c.cmd == null || handleWrite(c.cmd)) synchronized (sent) {
      sent.add(c);
      if (c.cmd != null) pending++;
      return true;
    } return false;
  }

  // Check if we can take a deferred command and send it.
  private boolean canSend() {
    return (level <= 0 || pending < level) && !deferred.isEmpty();
  }

  // Write deferred commands until we can't.
  private synchronized void writeDeferred() {
    while (canSend()) {
      if (tryWrite(deferred.peek()))
        deferred.removeFirst();
      else break;
    } notifyAll();
  }
}
