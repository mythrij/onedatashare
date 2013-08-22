package stork.scheduler;

import stork.*;
import stork.ad.*;
import stork.util.*;
import stork.module.*;
import stork.module.gridftp.*;
import stork.module.sftp.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

// The Stork transfer job scheduler. Maintains its own internal
// configuration and state as an ad. Operates based on commands given
// to it in the form of ads. Can be run standalone and receive commands
// from a number of interfaces, or can be run as part of a larger
// program and be given commands directly (or both).
//
// The entire state of the scheduler can be serialized and saved to disk,
// and subsequently recovered if so desired.
public class StorkScheduler {
  public Ad env = new Ad();
  public JobQueue job_queue = new JobQueue();
  public Map<String, StorkUser> users = new HashMap<String, StorkUser>();

  private transient Thread[] thread_pool;
  private transient Thread[] worker_pool;
  private transient Thread   dump_state_thread;

  private transient Map<String, StorkCommand> cmd_handlers;
  private transient TransferModuleTable xfer_modules;

  private transient LinkedBlockingQueue<RequestContext> req_queue =
    new LinkedBlockingQueue<RequestContext>();

  // Construct and return a usage/options parser object.
  public static GetOpts getParser(GetOpts base) {
    GetOpts opts = new GetOpts().parent(base);

    opts.prog = "server";
    opts.args = new String[] { "[option]..." };
    opts.desc = new String[] {
      "The Stork server is the core of the Stork system, handling "+
      "connections from clients and scheduling transfers. This command "+
      "is used to start a Stork server.",
      "Upon startup, the Stork server loads stork.conf and begins "+
      "listening for clients."
    };

    opts.add('d', "daemonize",
      "run the server in the background, redirecting output to a log "+
      "file (if specified)");
    opts.add('l', "log", "redirect output to a log file at PATH").parser =
      opts.new SimpleParser("log", "PATH", false);

    return opts;
  }

  // A thread which runs continuously and starts jobs as they're found.
  private class StorkQueueThread extends Thread {
    StorkQueueThread() {
      setDaemon(true);
    }

    // Continually remove jobs from the queue and start them.
    public void run() {
      while (true) try {
        StorkJob job = job_queue.take();
        Log.info("Pulled job from queue: "+job);

        // Make sure we didn't get interrupted while taking.
        if (job == null)
          continue;

        // Run the job then check the return status.
        switch (job.process()) {
          // If a job is still processing, something weird happened.
          case processing:
            throw new Exception("job still processing after completion");
          // If job is scheduled, put it back in the schedule queue.
          case scheduled:
            Log.info("Job "+job.jobId()+" rescheduling...");
            job_queue.schedule(job); break;
          // If the job was paused, put it in limbo until it's resumed.
          case paused:  // This can't happen yet!
            break;
          // Alert the user if it failed.
          case failed:
            Log.info("Job "+job.jobId()+" failed!");
        } dumpState();
      } catch (Exception e) {
        continue;
      }
    }
  }

  // A thread which handles client requests.
  private class StorkWorkerThread extends Thread {
    StorkWorkerThread() {
      setDaemon(true);
    }

    // Continually remove jobs from the queue and start them.
    public void run() {
      while (true) {
        RequestContext req;

        // Try to take something from the queue.
        try {
          req = req_queue.take();
          Log.fine("Worker pulled request from queue: "+req.cmd);
        } catch (Exception e) {
          continue;
        }

        // Try handling the command.
        try {
          if (req.cmd == null)
            throw new RuntimeException("no command specified");

          StorkCommand handler = cmd_handlers.get(req.cmd);

          if (handler == null)
            throw new RuntimeException("invalid command: "+req.cmd);

          // Check if the handler requires a logged in user.
          if (env.getBoolean("registration")) {
            if (handler.requiresLogin()) try {
              req.user = StorkUser.login(req.ad);
            } catch (RuntimeException e) {
              throw new RuntimeException(
                "action requires login: "+e.getMessage());
            }
          }

          req.ad.remove("pass_hash");

          // Let the magic happen.
          req.done(handler.handle(req));
        } catch (Exception e) {
          e.printStackTrace();
          String m = e.getMessage();
          req.done(new Ad("error", m == null ? e.toString() : m));
        } finally {
          Log.fine("Worker done with request: "+req.cmd);
        }
      }
    }
  }

  // Stork command handlers should implement this interface.
  static abstract class StorkCommand {
    public abstract Object handle(RequestContext req);

    // Override this for commands that don't require logon.
    public boolean requiresLogin() {
      return true;
    }
  }

  class StorkQHandler extends StorkCommand {
    public Ad handle(RequestContext req) {
      AdSorter sorter = new AdSorter("job_id");

      sorter.reverse(req.ad.getBoolean("reverse"));

      // Add jobs to the ad sorter.
      sorter.add(job_queue.get(req.ad).getAds());

      return (req.ad.getBoolean("count")) ? new Ad("count", sorter.size())
                                          : sorter.asAd();
    }
  }

  class StorkListHandler extends StorkCommand {
    public Ad handle(RequestContext req) {
      StorkSession sess = null;
      try {
        EndPoint ep = req.ad.unmarshalAs(EndPoint.class);
        sess = ep.session();
        return sess.list(ep.path(), req.ad);
      } finally {
        if (sess != null) sess.close();
      }
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  // Handle user registration.
  class StorkUserHandler extends StorkCommand {
    public StorkUser handle(RequestContext req) {
      if ("register".equals(req.ad.get("action", ""))) {
        StorkUser su = StorkUser.register(req.ad);
        Log.info("Registering user: "+su.user_id);
        dumpState();
        return su;
      } return StorkUser.login(req.ad);
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  // I cannot believe how simple this thing is for what it does.
  class StorkSubmitHandler extends StorkCommand {
    public Ad handle(RequestContext req) {
      Ad ad = job_queue.put(StorkJob.create(req.ad)).getAd();
      dumpState();
      return ad;
    }
  }

  // FIXME: This doesn't work right now; cause this to work.
  class StorkRmHandler extends StorkCommand {
    public Ad handle(RequestContext req) {
      Ad ad = req.ad;
      StorkJob j;
      String reason = "removed by user";
      Range r, cdr = new Range();

      if (!ad.has("range"))
        return new Ad("error", "no job_id range specified");

      r = Range.parseRange(ad.get("range"));

      if (r == null)
        return new Ad("error", "could not parse range");

      if (ad.has("reason"))
        reason = reason+" ("+ad.get("reason")+")";

      // Find ad in job list, set it as removed.
      for (int job_id : r) try {
        j = job_queue.get(job_id);
        j.remove(reason);
      } catch (IndexOutOfBoundsException oobe) {
        cdr.swallow(job_id);
      } catch (Exception e) {
        return new Ad("error", e.getMessage());
      }

      // See if there's anything in our "couldn't delete" range.
      if (cdr.size() == 0)
        return new Ad();
      if (cdr.size() == r.size())
        return new Ad("error", "no jobs were removed");
      return new Ad("message", "the following jobs weren't removed: "+cdr);
    }
  }

  class StorkInfoHandler extends StorkCommand {
    // Send transfer module information.
    Ad sendModuleInfo(RequestContext req) {
      return Ad.marshal(xfer_modules.infoAds());
    }

    // Send server information. But for now, don't send anything until we
    // know what sort of information is good to send.
    Ad sendServerInfo(RequestContext req) {
      return new Ad("error", "server info is currently unavailable");
    }

    public Ad handle(RequestContext req) {
      String type = req.ad.get("type", "module");

      if (type.equals("module"))
        return sendModuleInfo(req);
      if (type.equals("server"))
        return sendServerInfo(req);
      return new Ad("error", "invalid type: "+type);
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  // Iterate over libexec directory and add transfer modules to list.
  public void populateModules() {
    // Load built-in modules.
    // TODO: Automatic discovery for built-in modules.
    xfer_modules.register(new GridFTPModule());
    xfer_modules.register(new SFTPModule());
    if (env.has("libexec"))
      xfer_modules.registerDirectory(new File(env.get("libexec")));

    // Check if anything got added.
    if (xfer_modules.modules().isEmpty())
      Log.warning("no transfer modules registered");
  }

  // Initialize the thread pool according to config.
  // TODO: Replace worker threads with asynchronous I/O.
  public void initThreadPool() {
    int jn = env.getInt("max_jobs", 10);
    int wn = env.getInt("workers", 4);

    if (jn < 1) {
      jn = 10;
      Log.warning("invalid value for max_jobs, "+
                         "defaulting to "+jn);
    } if (wn < 1) {
      wn = 4;
      Log.warning("invalid value for workers, "+
                         "defaulting to "+wn);
    }

    thread_pool = new Thread[jn];
    worker_pool = new Thread[wn];
    
    Log.info("Starting "+jn+" job threads, and "+wn+" worker threads...");

    for (int i = 0; i < thread_pool.length; i++) {
      thread_pool[i] = new StorkQueueThread();
      thread_pool[i].start();
    } for (int i = 0; i < worker_pool.length; i++) {
      worker_pool[i] = new StorkWorkerThread();
      worker_pool[i].start();
    }
  }

  // Put a command in the server's request queue with an optional reply
  // bell and end bell.
  public RequestContext putRequest(Ad ad) {
    return putRequest(ad, null, null);
  } public RequestContext putRequest(Ad ad, Bell<Ad> reply_bell) {
    return putRequest(ad, reply_bell, null);
  } public RequestContext putRequest(Ad ad, Bell<Ad> rb, Bell<Ad> eb) {
    RequestContext rc = new RequestContext(ad, rb, eb);
    try {
      req_queue.put(rc);
    } catch (Exception e) {
      // Ugh...
    } return rc;
  }

  // Force the state dumping thread to dump the state.
  private synchronized StorkScheduler dumpState() {
    dump_state_thread.interrupt();
    return this;
  }

  // Thread which dumps server state periodically, or can be forced
  // to dump the server state.
  private class DumpStateThread extends Thread {
    public DumpStateThread() {
      setDaemon(true);
    }

    public void run() {
      while (true) {
        int delay = env.getInt("state_save_interval", 120);
        if (delay < 1) delay = 1;

        // Wait for the delay, then dump the state. Can be interrupted
        // to dump the state early.
        try {
          sleep(delay*1000);
        } catch (Exception e) {
          // Ignore.
        } dumpState();
      }
    }

    // Dump the state to the state file.
    private synchronized void dumpState() {
      String state_path = env.get("state_file");
      File state_file = null, temp_file = null;
      PrintWriter pw = null;

      if (state_path != null) try {
        state_file = new File(state_path).getAbsoluteFile();

        // Some initial sanity checks.
        if (state_file.exists()) {
          if (state_file.exists() && !state_file.isFile())
            throw new RuntimeException("state file is a directory");
          if (!state_file.canWrite())
            throw new RuntimeException("cannot write to state file");
        }

        temp_file = File.createTempFile(
          ".stork_state", "tmp", state_file.getParentFile());
        pw = new PrintWriter(temp_file, "UTF-8");

        pw.print(Ad.marshal(StorkScheduler.this));
        pw.close();
        pw = null;

        if (!temp_file.renameTo(state_file))
          throw new RuntimeException("could not rename temp file");
      } catch (Exception e) {
        Log.warning("couldn't save state: "+
                           state_file+": "+e.getMessage());
      } finally {
        if (temp_file != null && temp_file.exists()) {
          temp_file.deleteOnExit();
          temp_file.delete();
        } if (pw != null) try {
          pw.close();
        } catch (Exception e) {
          // Ignore.
        }
      }
    }
  }

  // Set the path for the state file.
  public synchronized StorkScheduler setStateFile(File f) {
    if (f != null)
      env.put("state_file", f.getAbsolutePath());
    else
      env.remove("state_file");
    return this;
  }

  // Load server state from a file.
  public StorkScheduler loadServerState(String f) {
    return loadServerState(f != null ? new File(f) : null);
  } public StorkScheduler loadServerState(File f) {
    if (f != null && f.exists()) {
      Log.info("Loading server state file: "+f);
      return loadServerState(Ad.parse(f));
    } return this;
  } public StorkScheduler loadServerState(Ad state) {
    try {
      state.unmarshal(this);
    } catch (Exception e) {
      Log.warning("Couldn't load server state: "+e.getMessage());
      e.printStackTrace();
    } return this;
  }

  // Restart a scheduler from saved state.
  public static StorkScheduler restart(String s) {
    return restart(new File(s));
  } public static StorkScheduler restart(File f) {
    return new StorkScheduler().loadServerState(f).init();
  }

  // Start a new scheduler with an optional config environment.
  public static StorkScheduler start() {
    return new StorkScheduler().init();
  } public static StorkScheduler start(File f) {
    return start(Ad.parse(f));
  } public static StorkScheduler start(Ad env) {
    StorkScheduler s = new StorkScheduler();

    if (env == null)
      env = new Ad();
    else if (env.has("state_file"))
      s.loadServerState(env.get("state_file"));

    s.env = env;

    return s.init();
  }

  private StorkScheduler init() {
    // Initialize command handlers
    cmd_handlers = new HashMap<String, StorkCommand>();
    cmd_handlers.put("q", new StorkQHandler());
    cmd_handlers.put("ls", new StorkListHandler());
    cmd_handlers.put("status", new StorkQHandler());
    cmd_handlers.put("submit", new StorkSubmitHandler());
    cmd_handlers.put("rm", new StorkRmHandler());
    cmd_handlers.put("info", new StorkInfoHandler());
    cmd_handlers.put("user", new StorkUserHandler());

    // Initialize transfer module set
    xfer_modules = TransferModuleTable.instance();

    // Initialize workers
    populateModules();
    initThreadPool();

    dump_state_thread = new DumpStateThread();
    dump_state_thread.start();
    dumpState();

    Log.info("Server state: "+Ad.marshal(this));

    return this;
  }

  // Don't allow these to be created willy-nilly.
  private StorkScheduler() { }
}
