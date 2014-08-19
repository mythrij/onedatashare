package stork.core.server;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import stork.ad.*;
import stork.core.*;
import stork.core.handlers.*;
import stork.cred.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

/**
 * The internal state of a Stork server. It should be possible to serialize
 * this one object and, in restoring it, recover the entire state of the
 * system. In other words, this class is the root of the state file. This is
 * how the Stork server maintains persistence across restarts and system
 * migrations.
 */
public class Server {
  public Config config = new Config();

  public Map<String,User> users = new HashMap<String,User>();

  public transient Scheduler scheduler = new Scheduler() {
    protected void schedule(Job job) {
      System.out.println(job);
    }
  };

  public transient ModuleTable modules = new ModuleTable();

  private transient DumpStateThread dumpStateThread;

  public transient Map<String, Class<? extends Handler>> handlers =
    new HashMap<String, Class<? extends Handler>>();

  private transient LinkedBlockingQueue<Request> requests =
    new LinkedBlockingQueue<Request>();

  private transient User anonymous = User.anonymous();

  // Map of idle sessions, for session reuse.
  public transient SessionCache sessions = new SessionCache();

  /** Pull a request from the queue and schedule it to be handled. */
  private void pullRequest() {
    
  }

  /** Get the handler for a command. */
  public Handler handlerFor(String command) {
    Class<? extends Handler> hc = handlers.get(command);
    if (hc == null) {
      throw new RuntimeException("Invalid command.");
    } try {
      Handler handler = hc.newInstance();
      handler.server = this;
      return handler;
    } catch (Exception e) {
      throw new RuntimeException("Server error.", e);
    }
  }

  /**
   * Get a request "form" for the given command. A form in this case is just
   * any object with fields that must be filled out, then returned to the
   * server via the {@link #issueRequest()} method.
   */
  public Request getRequestForm(String command) {
    return handlerFor(command).requestForm(command);
  }

  /** Put a request in the queue. */
  public Request issueRequest(final Request request) {
    if (request.handler == null) {
      request.ring(new Exception("Invalid command."));
    } else try {
      Log.fine("Enqueuing request: "+Ad.marshal(request));
      //requests.add(request);
      Bell.timerBell(0).new Promise() {
        public void done() { request.handle(); }
      };
    } catch (Exception e) {
      // This can happen if the queue is full. Which right now it never should
      // be, but who knows.
      Log.warning("Rejecting request: ", Ad.marshal(request));
      request.ring(new Exception("Rejected request"));
    } return request;
  }

  /** Load server state from a file. */
  public Server loadServerState(String f) {
    return loadServerState(f != null ? new File(f) : null);
  }

  /** Load server state from a file. */
  public Server loadServerState(File f) {
    if (f != null && f.exists()) {
      Log.info("Loading server state file: "+f);
      return loadServerState(Ad.parse(f));
    } return this;
  }

  /** Load server state from a file. */
  public Server loadServerState(Ad state) {
    try {
      state.unmarshal(this);
    } catch (Exception e) {
      Log.warning("Couldn't load server state: "+e.getMessage());
      e.printStackTrace();
    } return this;
  }

  /** Restart a server from saved state. */
  public synchronized void restart(String file) {
    restart(new File(file));
  }

  /** Restart a server from saved state. */
  public synchronized void restart(File file) {
    kill();
    loadServerState(file);
  }

  private void kill() {
    dumpStateThread.kill();
  }

  public Server(Config config) {
    handlers.put("q", QHandler.class);
    handlers.put("ls", ListHandler.class);
    handlers.put("mkdir", MkdirHandler.class);
    handlers.put("delete", DeleteHandler.class);
    handlers.put("status", QHandler.class);
    handlers.put("submit", SubmitHandler.class);
    handlers.put("cancel", CancelHandler.class);
    handlers.put("info", InfoHandler.class);
    handlers.put("user", UserHandler.class);
    handlers.put("cred", CredHandler.class);
    handlers.put("get", GetHandler.class);

    modules.populate();

    dumpStateThread = new DumpStateThread(config, this);
    dumpStateThread.dumpState();

    Log.info("Server state: "+Ad.marshal(this));
  }
}
