package stork.util;

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
  private final LinkedList<PipedCommand> pipe, sent;
  private final LinkedList<R> done;
  private int level;
  private PipedCommand current_cmd = null;
  protected Handler default_handler = null;
  private boolean dead = false;

  // Constructor
  public Pipeline(int l) {
    pipe = new LinkedList<PipedCommand>();
    sent = new LinkedList<PipedCommand>();
    done = new LinkedList<R>();
    setPipelining(l);
    setDaemon(true);
    start();
  } public Pipeline() {
    this(0);
  }

  // Internal representation of a command passed to the pipeliner.
  private class PipedCommand {
    C cmd;
    boolean ignore;
    Handler handler;

    PipedCommand(C c, boolean i, Handler h) {
      cmd = c; ignore = i;
      handler = (h != null) ? h : default_handler;
    }

    public String toString() {
      return "<"+cmd+">";
    }
  }

  // Interface for special command handler. Handlers, if present,
  // are run after a command is acknowledged.
  public abstract class Handler {
    // Overwrite this in subclasses to change write behavior.
    public abstract void handleWrite(C c) throws Exception;

    // Overwrite this in subclasses to change read behavior.
    public abstract void handleReply() throws Exception;
  }

  // Set the number of commands that are allowed to be piped without
  // acknowledgements. Treats n <= 0 as "infinite".
  public synchronized void setPipelining(int n) {
    level = (n <= 0) ? 0 : n;
  }

  // Should be run as a thread. Reads replies from sent commands with the
  // read handler then adds them to the done queue.
  public void run() {
    while (!dead) try {
      run2();
    } catch (Exception e) { /* Who cares... */ }
  }

  // Does the actual work of run().
  private synchronized void run2() throws Exception {
    getReply();
    while (!dead && send());
  }

  // Read a command from the pipe, send it, and add it to the sent queue.
  // Returns false if there's nothing to be sent.
  private synchronized boolean send() throws Exception {
    if (pipe.isEmpty()) return false;
    if (level > 0 && sent.size() >= level) return false;
    PipedCommand p = pipe.pop();

    try {
      p.handler.handleWrite(p.cmd);
    } catch (Exception e) {
      System.out.println("Send handler failed: "+p.cmd);
      e.printStackTrace();
    }

    sent.add(p);
    notifyAll();
    return true;
  }

  // Run the reply handler for the next command in the sent queue.
  private synchronized void getReply() throws Exception {
    while (!dead && sent.isEmpty()) wait();
    if (dead) return;
    current_cmd = sent.pop();

    try {
      current_cmd.handler.handleReply();
    } catch (Exception e) {
      e.printStackTrace();
    }

    notifyAll();
  }

  // Add a reply to the done list.
  public synchronized void addReply(R r) {
    if (current_cmd.ignore) return;
    done.add(r);
    notifyAll();
  }

  // Write a command to the queue to be piped. If ignore is specified, the
  // command's result will not be posted to the done queue.
  public synchronized void write(C c, boolean i, Handler h) throws Exception {
    pipe.add(new PipedCommand(c, i, h));
    while (send());  // Send all
  } public synchronized void write(C c, Handler handler) throws Exception {
    write(c, false, handler);
  } public synchronized void write(C c, boolean ignore) throws Exception {
    write(c, ignore, null);
  } public synchronized void write(C c) throws Exception {
    write(c, false);
  }

  // Pop a reply from the done queue.
  public synchronized R read() throws Exception {
    while (done.isEmpty()) wait();
    R r = done.pop();
    return r;
  }

  // Flush the whole pipe, then execute command.
  public synchronized R exchange(C c) throws Exception {
    flush(true);
    write(c);
    return read();
  }

  // Wait until every command is done.
  public synchronized void flush(boolean done_too) throws Exception {
    while (!pipe.isEmpty() || !sent.isEmpty()) wait();
    if (done_too) done.clear();
  } public synchronized void flush() throws Exception {
    flush(false);
  }

  // Kill the processing thread.
  public synchronized void kill() {
    if (!isAlive()) return;
    dead = true;
    notifyAll();
  }
}
