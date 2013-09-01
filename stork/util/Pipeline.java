package stork.util;

import stork.ad.*;
import java.util.*;

// A pipeline can be used by command-based protocols to send multiple
// commands across a channel without waiting for acknowledgment. The
// pipelining level specifies the number of unacknowledged commands that
// can be in the pipeline before more are allowed to be sent. If the
// pipeline is full, the write() method will block. The pipelining level
// can also be set to zero to do "infinite" pipelining.
//
// A pipelined command consists has a send handler, a receive handler, and
// an abort handler. The send handler actually handles issuing the command.
// The receive handler is called when it's the command's turn to have its
// acknowledgement reply handled. The abort handler allows the command to
// be canceled (if supported).

public abstract class Pipeline<C,R> extends Thread {
  private final LinkedList<PipedCommand> sent;
  private final LinkedList<Wrapper> done;
  private int level;
  private PipedCommand current_cmd = null;
  //protected Handler default_handler = null;
  private boolean dead = false;

  // This exception gets thrown whenever the pipe gets killed.
  public static final RuntimeException PIPELINE_ABORTED = null;

  // Constructor
  public Pipeline(int l) {
    super("pipeline");
    sent = new LinkedList<PipedCommand>();
    done = new LinkedList<Wrapper>();
    setPipelining(l);
    setDaemon(true);
    start();
  } public Pipeline() {
    this(0);
  }

  // Internal representation of a command passed to the pipeliner.
  private class PipedCommand {
    C cmd;
    Handler hnd;
    boolean ignore;
    Wrapper wrapper;

    PipedCommand(C c, Handler h, Wrapper w) {
      cmd = c; hnd = h; wrapper = w;
    }

    // If we're ignoring, keep handling till command is done.
    synchronized void handle() {
      if (cmd == null) {
        // It was a sync command.
        wrapper.set();
      } else try {
        R r = (hnd != null) ? hnd.handleReply() : handleReply();
        if (wrapper != null)
          wrapper.set(r);
      } catch (RuntimeException e) {
        wrapper.set(e);
      }
    }

    public String toString() {
      return "<"+cmd+">";
    }
  }

  // A proxy wrapper for replies or exceptions.
  // TODO: Replace with a bell.
  public class Wrapper {
    private R r = null;
    private RuntimeException e = null;
    private boolean set = false;

    Wrapper() { }
    Wrapper(R r) { this.r = r; set = true; }

    // Use this to wait for the wrapper to be set.
    public synchronized R get() {
      while (!set) try {
        wait();
      } catch (InterruptedException e) {
        if (dead) throw PIPELINE_ABORTED;
      } if (e != null) {
        throw e;
      } return r;
    }

    // Use these to set the wrapper value.
    private synchronized void set() {
      set = true;
      notifyAll();
    } public synchronized void set(R r) {
      this.r = r;
      set();
    } public synchronized void set(RuntimeException e) {
      this.e = e;
      set();
    }
  }

  // Implement write behavior in subclasses.
  public abstract void handleWrite(C c);

  public abstract R handleReply();

  public abstract class Handler {
    public abstract R handleReply();
  }

  // Set the number of commands that are allowed to be piped without
  // acknowledgements. Treats n <= 0 as "infinite".
  public synchronized void setPipelining(int n) {
    level = (n <= 0) ? 0 : n;
  }

  // Should be run as a thread. Reads replies from sent commands with the
  // read handler then adds them to the done queue.
  public void run() {
    while (!dead) {
      PipedCommand c;
      synchronized (this) {
        while (!dead && sent.isEmpty()) waitFor();
        if (dead) return;
        c = sent.pop();
        notifyAll();
      } c.handle();
    }
  }

  // Add a reply or exception to the done list.
  protected synchronized Wrapper addReplyProxy() {
    Wrapper w = new Wrapper();
    done.add(w);
    return w;
  } protected synchronized void addReply(R r) {
    done.add(new Wrapper(r));
  }

  // Write a command to the queue to be piped. If ignore is specified, the
  // command's result will not be posted to the done queue.
  public synchronized <H extends Handler> Wrapper write(C c, boolean i, H h) {
    while (level > 0 && sent.size() >= level)
      waitFor();

    if (c != null)
      handleWrite(c);

    Wrapper w = i ? new Wrapper() : addReplyProxy();

    sent.add(new PipedCommand(c, h, w));
    notifyAll();
    return w;
  } public <H extends Handler> Wrapper write(C c, H h) {
    return write(c, false, h);
  } public Wrapper write(C c, boolean ignore) {
    return write(c, ignore, null);
  } public Wrapper write(C c) {
    return write(c, false);
  }

  // Pop a reply from the done queue. If it was an exception, throw.
  public synchronized R read() {
    Wrapper r;
    synchronized (this) {
      while (done.isEmpty()) waitFor();
      r = done.pop();
      notifyAll();
    } return r.get();
  }

  // Execute a command synchronously.
  public R exchange(C c) {
    return write(c).get();
  }

  // Wait until every command is done.
  public void flush(boolean all) {
    write(null, true).get();
  } public void flush() {
    flush(false);
  }

  // Kill the processing thread.
  public synchronized void kill() {
    if (!isAlive()) return;
    dead = true;
    notifyAll();
    throw PIPELINE_ABORTED;
  }

  // wait() without the exception handling boilerplate.
  private void waitFor() {
    try {
      wait();
    } catch (Exception e) {
      Log.info("pipeline interrupted");
      kill();
    }
  }
}
