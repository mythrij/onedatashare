package stork.scheduler;

import stork.*;
import stork.ad.*;
import stork.util.*;
import stork.module.*;
import stork.module.gridftp.*;

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
// and subsequently recovered if so desired. Its entire state is stored as
// an ad, which is the reason for THIS. Is this the worst abuse of a data
// structure ever, or the best? :)  |
//                                  v
public class StorkScheduler extends Ad {
  // Singleton instance of the scheduler.
  private static StorkScheduler instance = null;

  // Server state variables
  private Thread[] thread_pool;
  private Thread[] worker_pool;

  private Map<String, StorkCommand> cmd_handlers;
  private TransferModuleTable xfer_modules;

  private JobQueue job_queue;
  private LinkedBlockingQueue<RequestContext> req_queue;

  private Bell<Integer> shutdown_bell = new Bell<Integer>();

  // Initialize the server state to a fresh state.
  public void initializeState() {
    //put("queue", new AdList());
    //put("users", StorkUser.globalMap());
  }

  // Construct and return a usage/options parser object.
  public static GetOpts getParser(GetOpts base) {
    GetOpts opts = new GetOpts(base);

    opts.prog = "stork_server";
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

  // Configuration variables
  private boolean daemon = false;
  private Ad env;

  // A thread which runs continuously and starts jobs as they're found.
  private class StorkQueueThread extends Thread {
    StorkQueueThread() {
      setDaemon(true);
    }

    // Continually remove jobs from the queue and start them.
    public void run() {
      while (true) try {
        StorkJob job = job_queue.take();
        System.out.println("Pulled job from queue: "+job);

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
            System.out.println("Job "+job.jobId()+" rescheduling...");
            job_queue.add(job); break;
          // If the job was paused, put it in limbo until it's resumed.
          case paused:  // This can't happen yet!
            break;
          // Alert the user if it failed.
          case failed:
            System.out.println("Job "+job.jobId()+" failed!");
        }
      } catch (Exception e) {
        System.out.println("Badness in StorkQueueThread...");
        e.printStackTrace();
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
          System.out.println("Pulled request from queue");
        } catch (Exception e) {
          System.out.println("Something bad happened in StorkWorkerThread...");
          e.printStackTrace();
          continue;
        }

        // Try handling the command.
        try {
          if (req.cmd == null)
            throw new FatalEx("no command specified");

          StorkCommand handler = cmd_handlers.get(req.cmd);

          if (handler == null)
            throw new FatalEx("invalid command: "+req.cmd);

          // Check if the handler requires a logged in user.
          if (handler.requiresLogin())
            req.user = StorkUser.login(req.ad);

          // Let the magic happen.
          req.done(handler.handle(req));
        } catch (Exception e) {
          e.printStackTrace();
          req.done(new ResponseAd("error", e.getMessage()));
        } finally {
          System.out.println("Done with request!");
        }
      }
    }
  }

  // Stork command handlers should implement this interface.
  static abstract class StorkCommand {
    public abstract Ad handle(RequestContext req);

    // Override for commands that don't require logon.
    public boolean requiresLogin() {
      return false;
    }
  }

  class StorkQHandler extends StorkCommand {
    public Ad handle(RequestContext req) {
      Ad ad = req.ad;
      boolean missed = false;

      // Show all jobs in range matching filter.
      List<StorkJob> jobs = job_queue.get(ad.get("range"), ad.get("status"));
      int count = jobs.size();

      if (count < 1)
        throw new FatalEx("no jobs found");

      for (StorkJob j : jobs)
        req.putReply(j);

      ResponseAd res = new ResponseAd("success");
      res.put("count", count);
      return res;
    }
  }

  class StorkListHandler extends StorkCommand {
    public Ad handle(RequestContext req) {
      StorkSession sess = null;
      try {
        EndPoint ep = new EndPoint(req.ad);
        sess = ep.session();
        return sess.list(ep.path, ep);
      } finally {
        if (sess != null) sess.close();
      }
    }
  }

  // Handle user registration.
  class StorkRegisterHandler extends StorkCommand {
    public Ad handle(RequestContext req) {
      System.out.println("Got registration ad: "+req.ad);
      StorkUser user = StorkUser.register(req.ad);
      return user;
    }
  }

  class StorkSubmitHandler extends StorkCommand {
    public Ad handle(RequestContext req) {
      StorkJob job;

      // Make sure the request has everything a StorkJob needs.
      job = new StorkJob(req.ad);

      // Add job to the job queue.
      job_queue.add(job);

      Ad res = new ResponseAd("success");
      res.put("job_id", job.jobId());
      return res;
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
        return new ResponseAd("error", "no job_id range specified");

      r = Range.parseRange(ad.get("range"));

      if (r == null)
        return new ResponseAd("error", "could not parse range");

      if (ad.has("reason"))
        reason = reason+" ("+ad.get("reason")+")";

      // Find ad in job list, set it as removed.
      for (int job_id : r) try {
        j = job_queue.get(job_id);
        j.remove(reason);
      } catch (IndexOutOfBoundsException oobe) {
        cdr.swallow(job_id);
      } catch (Exception e) {
        return new ResponseAd("error", e.getMessage());
      }

      // See if there's anything in our "couldn't delete" range.
      if (cdr.size() == 0)
        return new ResponseAd("success");
      if (cdr.size() == r.size())
        return new ResponseAd("error", "no jobs were removed");
      return new ResponseAd("success",
                            "the following jobs weren't removed: "+cdr);
    }
  }

  class StorkInfoHandler extends StorkCommand {
    // Send transfer module information
    Ad sendModuleInfo(RequestContext req) {
      for (TransferModule tm : xfer_modules.modules()) {
        try {
          req.putReply(tm.infoAd());
        } catch (Exception e) {
          return new ResponseAd("error", e.getMessage());
        }
      } return new ResponseAd("success");
    }

    // TODO: Send server information.
    Ad sendServerInfo(RequestContext req) {
      return new Ad(StorkScheduler.this);
    }

    public Ad handle(RequestContext req) {
      String type = req.ad.get("type", "module");

      if (type.equals("module"))
        return sendModuleInfo(req);
      if (type.equals("server"))
        return sendServerInfo(req);
      return new ResponseAd("error", "invalid type: "+type);
    }
  }

  // Iterate over libexec directory and add transfer modules to list.
  public void populateModules() {
    // Load built-in modules.
    // TODO: Not this...
    xfer_modules.register(new GridFTPModule());

    // Iterate over and populate external module list.
    // TODO: Do this in parallel and detect misbehaving externals.
    if (has("env.libexec")) {
      File dir = new File(get("env.libexec"));

      if (dir.isDirectory()) for (File f : dir.listFiles()) {
        // Skip over things that obviously aren't transfer modules.
        if (!f.isFile() || f.isHidden() || !f.canExecute())
          continue;

        try {
          xfer_modules.register(new ExternalModule(f));
        } catch (Exception e) {
          System.out.println("Warning: "+f+": "+e.getMessage());
          e.printStackTrace();
        }
      } else {
        System.out.println("Warning: libexec is not a directory!");
      }
    }

    // Check if anything got added.
    if (xfer_modules.modules().isEmpty())
      System.out.println("Warning: no transfer modules registered");
  }

  // Initialize the thread pool according to config.
  // TODO: Replace worker threads with asynchronous I/O.
  public void initThreadPool() {
    int jn = getInt("env.max_jobs", 10);
    int wn = getInt("env.workers", 4);

    if (jn < 1) {
      jn = 10;
      System.out.println("Warning: invalid value for max_jobs, "+
                         "defaulting to "+jn);
    } if (wn < 1) {
      wn = 4;
      System.out.println("Warning: invalid value for workers, "+
                         "defaulting to "+wn);
    }

    thread_pool = new Thread[jn];
    worker_pool = new Thread[wn];
    
    System.out.println("Starting "+jn+" job threads, "+
                       "and "+wn+" worker threads...");
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

  // Shut the server down gracefully and discard the instance.
  public synchronized void shutdown(int rv) {
    shutdown_bell.ring(rv);
    instance = null;
  }

  // Wait for the server to shutdown on its own, returning an error code
  // suitable to System.exit() with.
  public int waitFor() {
    while (true) try {
      return shutdown_bell.waitFor().intValue();
    } catch (Exception e) {
      // Just don't break.
    }
  }

  // Get the global instance of the StorkScheduler. Make one if it
  // doesn't exist.
  public static synchronized StorkScheduler instance(Ad env) {
    if (instance == null)
      instance = new StorkScheduler(env);
    return instance;
  }

  // Create a new scheduler from a serialized state.
  public StorkScheduler() {
    this(null);
  } public StorkScheduler(Ad state) {
    super(state);

    // Initialize command handlers
    cmd_handlers = new HashMap<String, StorkCommand>();
    cmd_handlers.put("stork_q", new StorkQHandler());
    cmd_handlers.put("stork_ls", new StorkListHandler());
    cmd_handlers.put("stork_status", new StorkQHandler());
    cmd_handlers.put("stork_submit", new StorkSubmitHandler());
    cmd_handlers.put("stork_rm", new StorkRmHandler());
    cmd_handlers.put("stork_info", new StorkInfoHandler());
    cmd_handlers.put("stork_register", new StorkRegisterHandler());

    // Initialize transfer module set
    xfer_modules = TransferModuleTable.instance();

    // Initialize queues
    job_queue = new JobQueue();
    req_queue = new LinkedBlockingQueue<RequestContext>();

    // Initialize workers
    populateModules();
    initThreadPool();

    // Set/get the user map.
    if (has("users") && typeOf("users") == AD)
      StorkUser.map(getAd("users"));
    put("users", StorkUser.map());
  }
}
