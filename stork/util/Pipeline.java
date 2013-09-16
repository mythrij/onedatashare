package stork.util;

import stork.ad.*;
import java.util.*;

// A pipeline can be used by command-based protocols to send multiple
// commands across a channel without waiting for acknowledgment. The
// pipelining level specifies the number of unacknowledged commands that
// can be in the pipeline before more are allowed to be sent. If the
// pipeline is full, the pipe() method will block. The pipelining level
// can also be set to zero to do "infinite" pipelining.
//
// A pipelined command consists of a send handler, a receive handler, and
// an abort handler. The send handler actually handles issuing the command.
// The receive handler is called when it's the command's turn to have its
// acknowledgement reply handled. The abort handler allows the command to
// be canceled (if supported).
//
// TODO: Actually implement the command abort functionality.

public abstract class Pipeline<C,R> extends Thread {
  private final LinkedList<PipeCommand> sent, deferred;
  private int pending = 0;
  private int level;
  private boolean dead = false;

  // This exception gets thrown whenever the pipe gets killed.
  public static final RuntimeException PIPELINE_ABORTED =
    new RuntimeException("pipeline aborted");

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
  private class PipeCommand {
    C cmd;
    Bell<R> bell;

    // A null command simply rings the bell when reached.
    PipeCommand(C c, Bell<R> b) {
      cmd = c; bell = b;
    }

    // Get the reply and ring the bell with it. If it's a sync (i.e. the
    // command is null), just ring it with nothing.
    boolean handle() {
      if (cmd == null) {
        return bell.ring();
      } else try {
        return bell.ring(handleReply(cmd), null);
      } catch (RuntimeException e) {
        Log.warning("Command failed: ", cmd);
        return bell.ring(null, e);
      }
    }

    // This gets called if the pipeline is killed.
    void kill() {
      if (!bell.isRung())
        bell.ring(PIPELINE_ABORTED);
    }

    public String toString() {
      return cmd.toString();
    }
  }

  // Implement write behavior in subclasses. Should return true if the
  // command was sent, or false to indicate we should call this again
  // later. It should only throw if the connection is closed.
  public abstract boolean handleWrite(C c);

  // Implement read behavior in subclasses. This should block until a reply
  // is received.
  public abstract R handleReply(C c);

  // Set the number of commands that are allowed to be piped without
  // acknowledgements. Treats n <= 0 as "infinite".
  public synchronized void setPipelining(int n) {
    level = (n <= 0) ? 0 : n;
  }

  // Should be run as a thread. Reads replies from sent commands with the
  // read handler then adds them to the done queue.
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
    } catch (Exception e) {
      // This probably means we died, but let the loop check.
      Log.warning("Pipeline thread caught exception", e);
      e.printStackTrace();
      kill();
    }
  }

  // Block until something has been deferred or sent.
  private synchronized void waitForWork() {
    while (sent.isEmpty() && deferred.isEmpty())
      waitFor();
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

  // Write a command to the queue to be piped.
  public synchronized Bell<R> pipe(C c, Bell<R> h) {
    h = (h == null) ? new Bell<R>() : h;
    return pipe(new PipeCommand(c, h));
  } public synchronized Bell<R> pipe(C c) {
    return pipe(c, null);
  } private synchronized Bell<R> pipe(PipeCommand c) {
    deferred.add(c);
    notifyAll();
    return c.bell;
  }

  // Wait until every command is done.
  public void sync() {
    pipe(null, null).waitFor();
  }

  // Kill the processing thread.
  public final synchronized void kill() {
    if (dead) return;
    Log.warning("Pipeline killed");
    Log.fine("Sent:     ", sent);
    Log.fine("Deferred: ", deferred);
    for (PipeCommand p : sent)     p.kill();
    for (PipeCommand p : deferred) p.kill();
    dead = true;
    notifyAll();
    throw PIPELINE_ABORTED;
  }

  // Wait forever unless the pipeline has died.
  private synchronized void waitFor() {
    if (dead) {
      throw PIPELINE_ABORTED;
    } else try {
      wait();
    } catch (Exception e) {
      if (dead) throw PIPELINE_ABORTED;
    }
  }
}
