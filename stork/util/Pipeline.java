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
  private final LinkedList<PipedCommand> sent;
  private final LinkedList<Wrapper> done;
  private int level;
  private PipedCommand current_cmd = null;
  protected Handler default_handler = null;
  private boolean dead = false;

  // Constructor
  public Pipeline(int l) {
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

    PipedCommand(C c, boolean i, Handler h) {
      cmd = c; ignore = i; hnd = h;
    }

    // If we're ignoring, keep handling till command is done.
    synchronized R handle() throws Exception {
      R r = (hnd != null) ? hnd.handleReply() : handleReply();
      return ignore ? null : r;
    }

    public String toString() {
      return "<"+cmd+">";
    }
  }

  // A wrapper for replies so exceptions can propagate correctly.
  private class Wrapper {
    R r = null; Exception e = null;
    Wrapper(R re, Exception ex) { r = re; e = ex; }
  }

  // Implement write behavior in subclasses.
  public abstract void handleWrite(C c) throws Exception;

  // Implement read behavior in subclasses. Return null when last reply
  // has been read to indicate command is complete. If an exception is
  // thrown, the assumption is the command failed and is complete.
  //
  // The "return null" method is a little hackish. Is there a better way?
  public abstract R handleReply() throws Exception;

  // Subclass this to implement a custom handler that will be executed
  // instead of handleReply().
  public abstract class Handler {
    public abstract R handleReply() throws Exception;
  }

  // Set the number of commands that are allowed to be piped without
  // acknowledgements. Treats n <= 0 as "infinite".
  public synchronized void setPipelining(int n) {
    level = (n <= 0) ? 0 : n;
  }

  // Should be run as a thread. Reads replies from sent commands with the
  // read handler then adds them to the done queue.
  public synchronized void run() {
    while (!dead) try {
      while (!dead && sent.isEmpty()) wait();
      if (!dead) try {
        R r = sent.pop().handle();
        if (r != null) addReply(r);
      } catch (Exception e) {
        addReply(e);
      } notifyAll();
    } catch (Exception e) {
      // wait() interrupted, ignore...
    }
  }

  // Add a reply or exception to the done list.
  protected synchronized void addReply(R r) {
    done.add(new Wrapper(r, null));
  } protected synchronized void addReply(Exception e) {
    done.add(new Wrapper(null, e));
  }

  // Write a command to the queue to be piped. If ignore is specified, the
  // command's result will not be posted to the done queue.
  public synchronized void write(C c, boolean i, Handler h)
  throws Exception {
    while (level > 0 && sent.size() >= level) wait();
    handleWrite(c);
    PipedCommand cmd = new PipedCommand(c, i, h);
    sent.add(cmd); notifyAll();
  } public void write(C c, Handler h) throws Exception {
    write(c, false, h);
  } public void write(C c, boolean ignore) throws Exception {
    write(c, ignore, null);
  } public void write(C c) throws Exception {
    write(c, false);
  }

  // Pop a reply from the done queue. If it was an exception, throw.
  public synchronized R read() throws Exception {
    while (done.isEmpty()) wait();
    Wrapper r = done.pop();
    notifyAll();
    if (r.e != null) throw r.e;
    return r.r;
  }

  // Flush the whole pipe, then execute command.
  public synchronized R exchange(C c) throws Exception {
    flush(true);
    write(c);
    return read();
  }

  // Wait until every command is done.
  public synchronized void flush(boolean done_too) throws Exception {
    while (!sent.isEmpty()) wait();
    if (done_too) while (!done.isEmpty()) read();  // Might throw.
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
