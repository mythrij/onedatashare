package stork.scheduler;

import stork.*;
import stork.ad.*;
import stork.cred.*;
import stork.module.*;
import stork.module.gridftp.*;
import stork.module.sftp.*;
import stork.user.*;
import stork.util.*;

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
//
// TODO: Break this thing up into separate classes.

public class StorkScheduler {
  public Ad env = new Ad();
  public User.Map users = new User.Map(this);

  public transient LinkedBlockingQueue<StorkJob> jobs =
    new LinkedBlockingQueue<StorkJob>();

  private transient StorkQueueThread[]  thread_pool;
  private transient StorkWorkerThread[] worker_pool;
  private transient Thread dump_state_thread;

  private transient Map<String, CommandHandler> cmd_handlers;
  public transient TransferModuleTable xfer_modules;

  private transient LinkedBlockingQueue<RequestBell> req_queue =
    new LinkedBlockingQueue<RequestBell>();

  private transient User anonymous = User.anonymous(this);

  // Put a job into the scheduling queue.
  public void schedule(StorkJob job) {
    jobs.add(job);
  }

  // It's like a thread, but storkier.
  private abstract class StorkThread<O> extends Thread {
    // Set these at any time to control the thread.
    public volatile boolean dead = false;
    public volatile boolean idle = true;

    public StorkThread(String name) {
      super(name);
      setDaemon(true);
      start();
    }

    public final void run() {
      while (!dead) try {
        O j = getAJob();
        idle = false;
        execute(j);
      } catch (Exception e) {
        continue;
      } finally {
        idle = true;
      }
    }

    public abstract O getAJob() throws Exception;
    public abstract void execute(O work);
  }

  // A thread which runs continuously and starts jobs as they're found.
  private class StorkQueueThread extends StorkThread<StorkJob> {
    StorkQueueThread() {
      super("stork queue thread");
    }

    public StorkJob getAJob() throws Exception {
      return jobs.take();
    }

    // Continually remove jobs from the queue and start them.
    public void execute(StorkJob job) {
      Log.info("Pulled job from queue: "+job);

      // Run the job then check the return status.
      switch (job.process()) {
        // If a job is still processing, something weird happened.
        case processing:
          throw new RuntimeException("job still processing after completion");
          // If job is scheduled, put it back in the schedule queue.
        case scheduled:
          Log.info("Job "+job.jobId()+" rescheduling...");
          schedule(job); break;
          // If the job was paused, put it in limbo until it's resumed.
        case paused:  // This can't happen yet!
          break;
          // Alert the user if it failed.
        case failed:
          Log.info("Job "+job.jobId()+" failed!");
      } dumpState();
    }
  }

  // A thread which handles client requests.
  private class StorkWorkerThread extends StorkThread<RequestBell> {
    StorkWorkerThread() {
      super("stork worker thread");
    }

    public RequestBell getAJob() throws Exception {
      return req_queue.take();
    }

    // Continually remove jobs from the queue and start them.
    public void execute(RequestBell req) {
      Log.fine("Worker pulled request from queue: ", req.cmd);
      Ad ad = null;

      // Try handling the command if it's not done already.
      if (!req.isRung()) try {
        if (req.cmd == null)
          throw new RuntimeException("No command specified.");

        // Check if user information was provided.
        if (req.ad.has("user")) try {
          req.user = users.login(req.ad.getAd("user"));
        } catch (Exception e) {
          if (req.handler.requiresLogin())
            throw new RuntimeException("Action requires login.");
          req.user = anonymous;
        } else {
          req.user = anonymous;
        }

        // Let the magic happen.
        ad = req.handler.handle(req);

        // Save state if we affected it.
        if (req.handler.affectsState(req))
          dumpState();

        req.ring(ad);
      } catch (Exception e) {
        e.printStackTrace();
        req.ring(e);
      } Log.fine("Worker done with request: ", req.cmd, ad);
    }
  }

  // Stork command handlers should implement this interface.
  public abstract class CommandHandler {
    public abstract Ad handle(RequestBell req);

    // Override this for commands that either don't require logging in or
    // always require logging in.
    public boolean requiresLogin() {
      return env.getBoolean("registration");
    }

    // Override this is the action affects server state.
    public boolean affectsState(RequestBell req) {
      return affectsState();
    } public boolean affectsState() {
      return false;
    }
  }

  class StorkQHandler extends CommandHandler {
    public Ad handle(RequestBell req) {
      List l = new JobSearcher(req.user.jobs).query(req.ad);

      return req.ad.getBoolean("count") ?
        new Ad("count", l.size()) : Ad.marshal(req.user.jobs);
    }
  }

  class StorkMkdirHandler extends CommandHandler {
    public Ad handle(RequestBell req) {
      StorkSession sess = null;
      try {
        EndPoint ep = new EndPoint(req.user, req.ad);
        sess = ep.session();
        sess.mkdir(ep.path());
        return new Ad("message", "Success!");
      } finally {
        if (sess != null) sess.close();
      }
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  class StorkRmfHandler extends CommandHandler {
    public Ad handle(RequestBell req) {
      StorkSession sess = null;
      try {
        EndPoint ep = new EndPoint(req.user, req.ad);
        sess = ep.session();
        sess.rm(ep.path());
        return new Ad("message", "Success!");
      } finally {
        if (sess != null) sess.close();
      }
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  class StorkLsHandler extends CommandHandler {
    public Ad handle(RequestBell req) {
      StorkSession sess = null;
      try {
        EndPoint ep = new EndPoint(req.user, req.ad);
        sess = ep.session();
        return Ad.marshal(sess.list(ep.path(), req.ad).waitFor());
      } finally {
        if (sess != null) sess.close();
      }
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  // Handle user registration.
  class StorkUserHandler extends CommandHandler {
    public Ad handle(RequestBell req) {
      if ("register".equals(req.ad.get("action"))) {
        User su = users.register(req.ad);
        Log.info("Registering user: ", su.email);
        return su.toAd();
      } if ("login".equals(req.ad.get("action"))) {
        return users.login(req.ad).toAd();
      } if ("history".equals(req.ad.get("action"))) {
        if (req.ad.has("uri")) try {
          req.user.addHistory(StorkUtil.makeURI(req.ad.get("uri")));
        } catch (Exception e) {
          throw new RuntimeException("Could not parse URI...");
        } return Ad.marshal(req.user.history);
      } return req.user.toAd();
    }

    public boolean requiresLogin() {
      return false;
    }

    public boolean affectsState(RequestBell req) {
      return "register".equals(req.ad.get("action"));
    } public boolean affectsState() {
      return true;
    }
  }

  class StorkSubmitHandler extends CommandHandler {
    public Ad handle(RequestBell req) {
      StorkJob job = StorkJob.create(req.user, req.ad);

      // Schedule the job to execute and add the job to the user context.
      schedule(job);

      synchronized (req.user) {
        req.user.jobs.add(job);
        job.jobId(req.user.jobs.size());
      }

      return job.getAd();
    }

    public boolean affectsState() {
      return true;
    }
  }

  class StorkRmHandler extends CommandHandler {
    public Ad handle(RequestBell req) {
      Range r = new Range(req.ad.get("range"));
      Range sdr = new Range(), cdr = new Range();

      if (r.isEmpty())
        throw new RuntimeException("No jobs specified.");

      // Find ad in job list, set it as removed.
      List<StorkJob> list = new JobSearcher(req.user.jobs).query(req.ad);
      for (StorkJob j : list) try {
        j.remove("removed by user");
        sdr.swallow(j.jobId());
      } catch (Exception e) {
        Log.info("Couldn't remove job ", j.jobId(), ": ", e.getMessage());
        cdr.swallow(j.jobId());
      }

      // See if there's anything in our "couldn't delete" range.
      if (sdr.isEmpty())
        throw new RuntimeException("No jobs were removed.");
      Ad ad = new Ad("removed", sdr.toString());
      if (!cdr.isEmpty())
        ad.put("not_removed", cdr.toString());
      return ad;
    }

    public boolean affectsState() {
      return true;
    }
  }

  class StorkInfoHandler extends CommandHandler {
    // Send transfer module information.
    Ad sendModuleInfo(RequestBell req) {
      return Ad.marshal(xfer_modules.infoAds());
    }

    // Send server information. But for now, don't send anything until we
    // know what sort of information is good to send.
    Ad sendServerInfo(RequestBell req) {
      Ad ad = new Ad();
      ad.put("version", Stork.version());
      ad.put("commands", new Ad(cmd_handlers.keySet()));
      return ad;
    }

    // Send information about a credential or about all credentials.
    Ad sendCredInfo(RequestBell req) {
      String uuid = req.ad.get("cred");
      if (uuid != null) try {
        return req.user.creds.getCred(uuid).getAd();
      } catch (Exception e) {
        throw new RuntimeException("no credential could be found");
      } else {
        return req.user.creds.getCredInfos(req.ad.get("user_id"));
      }
    }

    public Ad handle(RequestBell req) {
      String type = req.ad.get("type", "module");

      if (type.equals("module"))
        return sendModuleInfo(req);
      if (type.equals("server"))
        return sendServerInfo(req);
      if (type.equals("cred"))
        return sendCredInfo(req);
      return new Ad("error", "invalid type: "+type);
    }

    public boolean requiresLogin() {
      return false;
    }
  }

  // Handles creating credentials.
  class StorkCredHandler extends CommandHandler {
    public Ad handle(RequestBell req) {
      String action = req.ad.get("action");

      if (action == null) {
        throw new RuntimeException("no action specified");
      } if (action.equals("create")) {
        StorkCred<?> cred = StorkCred.create(req.ad);
        String uuid = req.user.creds.add(cred);
        return cred.getAd().put("uuid", uuid);
      } throw new RuntimeException("invalid action");
    }

    public boolean affectsState() {
      return true;
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

    thread_pool = new StorkQueueThread[jn];
    worker_pool = new StorkWorkerThread[wn];
    
    Log.info("Starting "+jn+" job threads, and "+wn+" worker threads...");

    for (int i = 0; i < thread_pool.length; i++) {
      thread_pool[i] = new StorkQueueThread();
    } for (int i = 0; i < worker_pool.length; i++) {
      worker_pool[i] = new StorkWorkerThread();
    }

    // Let's go ahead and start one of these things. This is a quick hack
    // to make sure worker threads don't get stuck on long calls.
    new Thread("stork sentinel") {
      public void run() {
        while (true) try {
          // Sleep for a bit and make sure there's a free thread.
          sleep(100);
          check(worker_pool, StorkWorkerThread.class);
          //check(thread_pool, StorkQueueThread.class);
        } catch (Exception e) {
          // I doubt this will happen.
        }
      } public <C extends StorkThread<?>> void check(C[] pool, Class<C> c) {
        for (C t : pool) if (t.idle) return;
        int i = (int) (Math.random() * pool.length);
        Log.info("Retiring ", c, " #", i);
        C z;
        if (c == StorkWorkerThread.class)
          z = c.cast(new StorkWorkerThread());
        else if (c == StorkQueueThread.class)
          z = c.cast(new StorkQueueThread());
        else
          return;
        pool[i].dead = true;
        pool[i] = z;
        Log.info("Created new ", c);
      }
    }.start();
  }

  // Put a command in the server's request queue with an optional reply
  // bell and end bell.
  public RequestBell putRequest(Ad ad) {
    return putRequest(new RequestBell(ad));
  } public RequestBell putRequest(RequestBell rb) {
    assert rb.ad != null;
    rb.handler = handler(rb.cmd);
    if (rb.handler == null) {
      rb.ring(new Exception("Invalid command: "+rb.cmd));
    } else try {
      Log.fine("Enqueuing request: "+rb.ad);
      req_queue.add(rb);
    } catch (Exception e) {
      // This can happen if the queue is full. Which right now it never
      // should be, but who knows.
      Log.warning("Rejecting request: ", rb.ad);
      rb.ring(new Exception("Rejected request"));
    } return rb;
  }

  // Get the handler for a command.
  public CommandHandler handler(String cmd) {
    return cmd_handlers.get(cmd);
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
      super("server dump thread");
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

  // Unmarshal data from an ad.
  private void unmarshalFrom(Ad ad) {
    // Add all of the users.
    Ad ua = ad.getAd("users");
    if (ua != null) for (String s : ua.keySet()) {
      System.out.println(ua.getAd(s));
      User u = new User(this, ua.getAd(s));
      users.insert(u);

      // Add their unfinished jobs.
      if (u.jobs != null) for (StorkJob j : u.jobs)
        if (!j.isTerminated()) schedule(j.status(JobStatus.scheduled));
    }
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
      unmarshalFrom(state);
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
    cmd_handlers = new HashMap<String, CommandHandler>();
    cmd_handlers.put("q", new StorkQHandler());
    cmd_handlers.put("ls", new StorkLsHandler());
    cmd_handlers.put("mkdir", new StorkMkdirHandler());
    cmd_handlers.put("rmf", new StorkRmfHandler());
    cmd_handlers.put("status", new StorkQHandler());
    cmd_handlers.put("submit", new StorkSubmitHandler());
    cmd_handlers.put("rm", new StorkRmHandler());
    cmd_handlers.put("info", new StorkInfoHandler());
    cmd_handlers.put("user", new StorkUserHandler());
    cmd_handlers.put("cred", new StorkCredHandler());

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
