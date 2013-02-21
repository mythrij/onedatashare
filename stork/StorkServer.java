package stork;

import stork.*;
import stork.util.*;
import stork.module.*;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.channels.spi.*;
import java.util.*;
import java.util.concurrent.*;

// TODO: Search FIXME and TODO! Also, make everything not static.

public class StorkServer implements Runnable {
  // Server state variables
  private ServerSocket listen_sock = null;

  private Thread[] thread_pool;
  private Thread[] worker_pool;

  private Map<String, StorkCommand> cmd_handlers;
  private TransferModuleTable xfer_modules;

  private ArrayList<StorkJob> all_jobs;
  private LinkedBlockingQueue<StorkJob> job_queue;
  private LinkedBlockingQueue<ClientRequest> req_queue;

  boolean connected = false;

  // Construct and return a usage/options parser object.
  public static GetOpts getParser(GetOpts base) {
    GetOpts opts = new GetOpts(base);

    opts.prog = "stork_server";
    opts.args = new String[] { "[option]..." };
    opts.desc = new String[] {
      "The Stork server is the core of the Stork system, handling "+
      "connections from clients and scheduling transfers. This command "+
      "is used to start a Stork server.",
      "Upon startup, the Stork server loads stork.env and begins "+
      "listening for clients."
    };

    opts.add('d', "daemonize",
      "run the server in the background, redirecting output to a log "+
      "file (if specified)");
    opts.add('l', "log", "redirect output to a log file at PATH").parser =
      opts.new SimpleParser("log", "PATH", false);

    return opts;
  }

  // Used to determine time relative to start of server.
  // TODO: Not this...
  private long server_date = System.currentTimeMillis();
  private long server_mt_base = System.nanoTime() / (long)1E6;

  // Configuration variables
  private boolean daemon = false;
  private Ad env;

  // States a job can be in.
  static enum JobStatus {
    scheduled, processing, removed, failed, complete
  }

  // Filters for jobs of certain types.
  static class JobFilter {
    static EnumSet<JobStatus>
      all = EnumSet.allOf(JobStatus.class),
      pending = EnumSet.of(JobStatus.scheduled,
                           JobStatus.processing),
      done = EnumSet.complementOf(JobFilter.pending);
  }

  // A class for looking up transfer modules by protocol and handle.
  class TransferModuleTable {
    Map<String, TransferModule> by_proto, by_handle;

    public TransferModuleTable() {
      by_proto  = new HashMap<String, TransferModule>();
      by_handle = new HashMap<String, TransferModule>();
    }

    // Add a transfer module to the table.
    public void register(TransferModule tm) {
      if (tm == null) {
        System.out.println("Error: register called with null argument");
        return;
      }

      // Check if handle is in use.
      if (!by_handle.containsKey(tm.handle())) {
        by_handle.put(tm.handle(), tm);
        System.out.println(
          "Registered module \""+tm+"\" [handle: "+tm.handle()+"]");
      } else {
        System.out.println(
          "Warning: module handle "+tm.handle()+" in use, ignoring");
        return;
      }

      // Add the protocols for this module.
      for (String p : tm.protocols()) {
        if (!by_proto.containsKey(p)) {
          System.out.println("  Registering protocol: "+p);
          by_proto.put(p, tm);
        } else {
          System.out.println(
            "  Note: protocol "+p+" already registered, not registering");
          continue;
        }
      }
    }

    // Get a transfer module based on source and destination protocol.
    public TransferModule lookup(String sp, String dp) {
      TransferModule tm = by_proto.get(sp);

      // TODO: Cross-module lookup.
      if (tm != by_proto.get(dp))
        return null;
      return tm;
    }

    // Get a transfer module by its handle.
    public TransferModule lookup(String handle) {
      return by_handle.get(handle);
    }

    // Get a set of all the modules.
    public Collection<TransferModule> modules() {
      return by_handle.values();
    }

    // Get a set of all the handles.
    public Collection<String> handles() {
      return by_handle.keySet();
    }

    // Get a set of all the supported protocols.
    public Collection<String> protocols() {
      return by_proto.keySet();
    }
  }

  // A representation of a job submitted to Stork. When this job is
  // run by a thread, start the transfer, and read aux_ads from it
  // until the job completes.
  // TODO: Make this less hacky with the filters and such. Also, should
  // this be in a separate class?
  class StorkJob implements Runnable {
    JobStatus status;
    SubmitAd job_ad;
    Ad aux_ad;
    StorkTransfer transfer;
    TransferModule tm;

    int job_id = 0;
    int attempts = 0, rv = -1;
    String message = null;
    long submit_time, start_time = -1;
    int run_duration = -1;

    // Set me to null to force regeneration of info ad by getAd().
    Ad cached_ad = null;

    // Create a StorkJob from a job ad.
    public StorkJob(SubmitAd ad) {
      job_ad = ad;
      tm = ad.tm;
      set_status(JobStatus.scheduled);

      // Set the submit time for the job
      submit_time = get_server_time();
    }

    // Gets the job info as a ad. Will return cached_ad if there is one.
    // To force regeneration of ad, reset cached_ad to null.
    public synchronized Ad getAd() {
      Ad ad = cached_ad;

      // If we don't have a cached ad, generate a new one.
      if (ad == null) {
        ad = new Ad(job_ad);

        // Remove sensitive stuff.
        // TODO: Better method...
        ad.remove("x509_proxy");

        // Merge auxilliary ad if there is one.
        // TODO: Real filtering.
        if (aux_ad != null) {
          ad.merge(aux_ad);
          ad.remove("error");  // Don't misbehave!
        }

        // Add other job information.
        ad.put("status", status.toString());
        ad.put("module", tm.handle());

        if (job_id > 0)
          ad.put("job_id", job_id);
        if (attempts > 0)
          ad.put("attempts", attempts);
        if (message != null)
          ad.put("message", message);
        if (run_duration >= 0)
          ad.put("run_duration", pretty_time(run_duration));
      }

      if (rv >= 0)
        ad.put("exit_status", rv);

      // Add elapsed time if processing
      if (status == JobStatus.processing)
        ad.put("run_duration", pretty_time(since(start_time)));
      
      return cached_ad = ad;
    }

    // Given date in ms, return ms elapsed.
    // XXX: Kinda hacky to have this here...
    private int since(long t) {
      if (t < 0) return -1;
      return (int) (get_server_time() - t);
    }

    // Given a duration in ms, return a pretty string representation.
    private String pretty_time(long t) {
      if (t < 0) return null;

      long i = t % 1000,
           s = (t/=1000) % 60,
           m = (t/=60) % 60,
           h = (t/=60) % 24,
           d = t / 24;

      return (d > 0) ? String.format("%dd%02dh%02dm%02ds", d, h, m, s) :
             (h > 0) ? String.format("%dh%02dm%02ds", h, m, s) :
             (m > 0) ? String.format("%dm%02ds", m, s) :
                       String.format("%d.%02ds", s, i/10);
    }

    // Sets the status of the job and updates the ad accordingly.
    public synchronized void set_status(JobStatus s) {
      status = s;

      if (cached_ad != null)
        cached_ad.put("status", s.toString());
    }

    // Set job message. Pass null to remove message.
    public synchronized void set_message(String m) {
      message = m;

      if (cached_ad != null)
        cached_ad.put("message", m);
    }

    // Set the attempts counter.
    public synchronized void set_attempts(int a) {
      attempts = a;

      if (cached_ad != null) {
        if (a != 0)
          cached_ad.put("attempts", a);
        else
          cached_ad.remove("attempts");
      }
    }

    // Called when the job gets removed. Returns true if the job had its
    // state updated, and false otherwise (e.g., the job was already
    // complete or couldn't be removed).
    public synchronized boolean remove(String reason) {
      switch (status) {
        // Try to stop the job. If we can't, don't do anything.
        case processing:
          if (transfer != null) transfer.stop();
          run_duration = since(start_time);
          transfer = null;

        // Fall through to set removed status.
        case scheduled:
          set_message(reason);
          set_status(JobStatus.removed);

          return true;

        // In any other case, the job has ended, do nothing.
        default:
          return false;
      }
    }

    // Check if the job should be rescheduled.
    public synchronized boolean shouldReschedule() {
      // Check for forced rescheduling prevention.
      if (rv >= 255)
        return false;

      // Check for custom max attempts.
      int max = job_ad.getInt("max_attempts", 10);
      if (max > 0 && attempts >= max)
        return false;

      // Check for configured max attempts.
      max = env.getInt("max_attempts", 10);
      if (max > 0 && attempts >= max)
        return false;

      return true;
    }

    // Run the job and watch it to completion.
    public void run() {
      // Must be scheduled to be able to run.
      if (status != JobStatus.scheduled)
        return;

      // Start transfer
      synchronized (status) {
        start_time = get_server_time();
        set_status(JobStatus.processing);
        transfer = tm.transfer(job_ad);
        transfer.start();
      }

      // Read progress ads until end of ad stream.
      while (true) try {
        Ad ad = transfer.getAd();

        aux_ad = ad;

        // Check if we have message; unset if present and empty string.
        message = aux_ad.get("message", message);
        if (message != null && message.isEmpty())
          message = null;

        cached_ad = null;  // Blow out the cached ad.
      } catch (Exception e) {
        break;
      }

      // Wait for job to complete and get exit status.
      rv = transfer.waitFor();
      transfer = null;
      run_duration = since(start_time);

      if (rv == 0) {  // Job successful!
        set_status(JobStatus.complete);
        transfer = null;
        return;
      }

      // Job not successful! :( Check if we should requeue.
      if (shouldReschedule()) {
        set_status(JobStatus.scheduled);

        set_attempts(attempts+1);
        System.out.println("Job "+job_id+" failed! Rescheduling...");
        transfer = null;

        try {
          job_queue.put(this);
        } catch (Exception e) {
          System.out.println("Error rescheduling job "+job_id+": "+e);
        }
      } else {
        System.out.println("Job "+job_id+" failed!");
        transfer = null;
        set_status(JobStatus.failed);
        set_attempts(attempts);
      }
    }
  }

  // A thread which runs continuously and starts jobs as they're found.
  // TODO: Refactor me.
  private class StorkQueueThread extends Thread {
    StorkQueueThread() {
      setDaemon(true);
    }

    // Continually remove jobs from the queue and start them.
    public void run() {
      while (true) try {
        StorkJob job = job_queue.take();
        System.out.println("Pulled job from queue");

        // Some sanity checking
        if (job.status != JobStatus.scheduled) {
          System.out.println("How did this get here?! ("+job+")");
          continue;
        }

        job.run();
      } catch (Exception e) {
        System.out.println("Something bad happened in StorkQueueThread...");
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
      while (true) try {
        ClientRequest req = req_queue.take();
        System.out.println("Pulled request from queue");
        req.handle();
        System.out.println("Done with request!");
      } catch (Exception e) {
        System.out.println("Something bad happened in StorkQueueThread...");
        e.printStackTrace();
      }
    }
  }

  // Stork command handlers should implement this interface.
  static interface StorkCommand {
    public Ad handle(ClientRequest req);
  }

  class StorkQHandler implements StorkCommand {
    public Ad handle(ClientRequest req) {
      // The list we will ultimately read results from.
      Iterable<StorkJob> list = all_jobs;
      EnumSet<JobStatus> filter = null;
      Ad ad = req.ad;

      String type = ad.get("status");
      Range range, nfr = new Range();
      int count = 0;
      boolean missed = false;

      // Lowercase the job type just to make things easier.
      if (type != null)
        type = type.toLowerCase();

      // Pick a filter depending on type. Valid types include:
      // "pending", "done", "all", and any job status. Defaults to
      // "pending" if no range specified, "all" if it is specified.
      if (type == null) {
        filter = ad.has("range") ? JobFilter.all : JobFilter.pending;
      } else if (type.equals("pending")) {
        filter = JobFilter.pending;
      } else if (type.equals("done")) {
        filter = JobFilter.done;
      } else if (type.equals("all")) {
        filter = JobFilter.all;
      } else try {
        filter = EnumSet.of(JobStatus.valueOf(type));
      } catch (Exception e) {
        return new ResponseAd("error", "invalid job type '"+type+"'");
      }

      // If a range was given, show jobs from range.
      if (ad.has("range"))
        range = Range.parseRange(ad.get("range"));
      else
        range = new Range(1, all_jobs.size());
      if (range == null)
        return new ResponseAd("error", "could not parse range");

      // Show all jobs in range matching filter.
      for (int i : range) try {
        StorkJob j = all_jobs.get(i-1);

        if (!filter.contains(j.status))
          continue; 
        count++;

        System.out.println("Sending: "+j.getAd());
        req.add(j.getAd());
      } catch (IndexOutOfBoundsException oobe) {
        missed = true;
        nfr.swallow(i);
      } catch (Exception e) {
        return new ResponseAd("error", e.getMessage());
      }

      // Inform user of count and any missing jobs.
      ResponseAd res = new ResponseAd("success");

      if (!nfr.isEmpty()) {
        if (count == 0)
          res.set("error", "no jobs found");
        else
          res.put("not_found", nfr.toString());
      }

      res.put("count", count);
      return res;
    }
  }

  class StorkListHandler implements StorkCommand {
    public Ad handle(ClientRequest req) {
      return null;
    }
  }

  class StorkSubmitHandler implements StorkCommand {
    public Ad handle(ClientRequest req) {
      SubmitAd sad;

      // Try to interpret ad as a submit ad.
      try {
        sad = new SubmitAd(req.ad);
        TransferModule tm;
        String handle = StorkUtil.normalize(sad.get("module"));

        // See if job specially requests a module.
        if (!handle.isEmpty()) {
          tm = xfer_modules.lookup(handle);

          if (tm == null)
            throw new Exception("no module with name \""+handle+"\"");
        } else {
          // Check for transfer modules for URLs.
          // TODO: Check for inter-module transfers later.
          String sp = sad.src_proto, dp = sad.dest_proto;
          tm = xfer_modules.lookup(sp, dp);

          if (tm == null)
            throw new Exception("cannot transfer "+sp+" -> "+dp);
        }

        // Make sure transfer module likes job ad.
        sad.setModule(tm);
      } catch (Exception e) {
        return new ResponseAd("error", e.getMessage());
      }

      StorkJob job = new StorkJob(sad);

      // Check that is ready to be processed.
      if (job.status != JobStatus.scheduled)
        return new ResponseAd("error", job.message);

      // Add job to the job log and determine job id
      synchronized (all_jobs) {
        all_jobs.add(job);
        job.job_id = all_jobs.size();
      }

      // Add to the scheduler
      try {
        job_queue.put(job);
      } catch (Exception e) {
        System.out.println("Error scheduling job "+job.job_id+": "+e);
      }

      Ad res = new ResponseAd("success");
      res.put("job_id", job.job_id);
      return res;
    }
  }

  class StorkRmHandler implements StorkCommand {
    public Ad handle(ClientRequest req) {
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
        j = all_jobs.get(job_id-1);
        job_queue.remove(j);
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
      return new ResponseAd("error",
                            "the following jobs weren't removed: "+cdr);
    }
  }

  class StorkInfoHandler implements StorkCommand {
    // Send transfer module information
    Ad sendModuleInfo(ClientRequest req) {
      for (TransferModule tm : xfer_modules.modules()) {
        try {
          req.add(tm.infoAd());
        } catch (Exception e) {
          return new ResponseAd("error", e.getMessage());
        }
      } return new ResponseAd("success");
    }

    // TODO: Send server information.
    Ad sendServerInfo(ClientRequest req) {
      return new ResponseAd("error", "not yet implemented");
    }

    public Ad handle(ClientRequest req) {
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
    File dir = new File(env.get("libexec"));

    // Load built-in modules.
    // TODO: Not this...
    xfer_modules.register(new StorkGridFTPModule());

    // Iterate over and populate external module list.
    // TODO: Do this in parallel and detect misbehaving externals.
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

    // Check if anything got added.
    if (xfer_modules.modules().isEmpty())
      System.out.println("Warning: no transfer modules registered");
  }

  // Get the current Unix date in ms based on server time.
  // TODO: Move this somewhere else.
  long get_server_time() {
    long dtime = System.nanoTime() / (long)1E6 - server_mt_base;
    return server_date + dtime;
  }

  ServerSocketChannel channel;

  // Create and bind a server socket channel for this StorkServer.
  public Selector connect(String host, int port) throws Exception {
    ServerSocket sock;
    InetSocketAddress addr;
    Selector selector = SelectorProvider.provider().openSelector();

    // Create selector and channel and get associated socket.
    channel = ServerSocketChannel.open();
    channel.configureBlocking(false);
    sock = channel.socket();

    // Bind socket to the given host/port.
    if (host == null) host = "0.0.0.0";
    addr = new InetSocketAddress(host, port);
    sock.bind(addr);
    port = sock.getLocalPort();

    // Register channel to selector.
    channel.register(selector, SelectionKey.OP_ACCEPT);

    System.out.printf("Listening on %s:%d...\n", host, port);
    return selector;
  }

  // Initialize the thread pool according to config.
  // TODO: Replace worker threads with asynchronous I/O.
  public void initThreadPool() {
    int jn = env.getInt("max_jobs", 10);
    int wn = env.getInt("workers", 4);

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

  // Represents a client request to be handled. Maintains a list
  // of ads to be sent to the client.
  private class ClientRequest {
    boolean done = false;
    SelectionKey key;
    Ad ad;
    String cmd;
    StorkCommand handler = null;
    LinkedList<Ad> list;
    ByteBuffer remainder = null;

    ClientRequest(SelectionKey key, Ad cmd_ad) {
      this.key = key;
      cmd = cmd_ad.get("command");
      ad = cmd_ad.remove("command");
      list = new LinkedList<Ad>();
      if (cmd != null) handler = cmd_handlers.get(cmd);
    }

    // Call the handler for the request and finalize.
    synchronized void handle() {
      if (cmd == null)
        done(new ResponseAd("error", "no command specified"));
      else if (handler == null)
        done(new ResponseAd("error", "invalid command: "+cmd));
      else
        done(handler.handle(this));
    }

    // Return the first ad in the response queue.
    synchronized Ad peek() {
      if (list.size() <= 0)
        return null;
      return list.peek();
    }

    // Remove and return the first ad in the response queue.
    synchronized Ad pop() {
      if (list.size() <= 0)
        return null;
      if (list.size() == 1)
        key.interestOps(0);
      return list.pop();
    }

    // Put an ad into the response queue.
    synchronized void add(Ad ad) {
      System.out.println("Adding: "+ad);
      if (done) return;
      list.add(ad);
      key.interestOps(key.OP_WRITE);
      key.selector().wakeup();
    }

    // Called whenever a request has been served and the next command
    // should be read from the client and handled.
    synchronized boolean done() {
      return done && list.isEmpty();
    } synchronized void done(Ad last) {
      if (done) return;
      if (last != null) add(last);
      done = true;
    }

    // Write the response queue to a channel.
    // TODO: Allow adds while writing.
    synchronized void write(SocketChannel sc) throws Exception {
      Ad ad;
      System.out.println("Writing");

      // Write the remainder if there is any.
      if (remainder != null) {
        System.out.println("Writing remainder...");
        sc.write(remainder);
        if (remainder.remaining() > 0) return;
        remainder = null;
      }

      // Write ads until we're out or can't anymore.
      while ((ad = pop()) != null) {
        System.out.println("Writing ad: "+ad);
        remainder = ByteBuffer.wrap(ad.serialize());
        sc.write(remainder);
        if (remainder.remaining() > 0) return;
        remainder = null;
      }

      // We're out of ads, so disable write until later.
      key.interestOps(0);
    }
  }

  // Cancel a key in a selector.
  private static void cancelKey(SelectionKey k) {
    try {
      k.cancel();
      ((SocketChannel) k.channel()).close();
    } catch (Exception e) {
      // We don't really care that much...
    }
  }

  // Handle selection keys from our selector.
  public void readCommand(SelectionKey k) {
    if (!k.isValid()) return;

    SocketChannel sc = (SocketChannel) k.channel();
    Ad.Parser par = (Ad.Parser) k.attachment();
    ByteBuffer bb = ByteBuffer.allocate(5000);
    int len;

    if (par == null)
      k.attach(par = new Ad.Parser());

    // Try to read data from socket.
    try {
      len = sc.read(bb);
    } catch (Exception e) {
      System.out.println("Something nasty happened...");
      cancelKey(k);
      return;
    }

    // See if we got a disconnect.
    if (len > 0) try {
      String str = new String(bb.array(), 0, len);
      Ad ad = par.write(str);

      // Null means more data is required by the parser.
      if (ad == null)
        return;
      
      System.out.println("Got ad: "+ad);
      k.interestOps(0);
      k.selector().wakeup();

      // Queue up request, unless there's an issue.
      ClientRequest cr = new ClientRequest(k, ad);
      if (cr.handler != null) {
        k.attach(cr); req_queue.add(cr);
      }
    } catch (Exception e) {
      System.out.println("Got bad input: "+e.getMessage());
      cancelKey(k);
    } else if (len < 0) {
      cancelKey(k);
    }
  }

  // Handle writing responses to the client.
  public void writeResponse(SelectionKey k) {
    if (!k.isValid()) return;

    SocketChannel sc = (SocketChannel) k.channel();
    ClientRequest rq = (ClientRequest) k.attachment();

    // See if we're done first.
    if (rq == null || rq.done()) return;

    try {
      rq.write(sc);
    } catch (Exception e) {
      System.out.println("Uh oh: "+e);
      e.printStackTrace();
    }

    if (rq.done()) {
      k.attach(null);
      k.interestOps(k.OP_READ);
      k.selector().wakeup();
    }
  }

  // Handle accepting a new client.
  public void acceptClient(SelectionKey k) {
    if (!k.isValid()) return;

    try {
      ServerSocketChannel ssc = (ServerSocketChannel) k.channel();
      SocketChannel sc = ssc.accept();
      if (sc != null) {
        sc.configureBlocking(false);
        sc.register(k.selector(), k.OP_READ);
      }
    } catch (Exception e) {
      System.out.println("Warning: caught something weird...");
      e.printStackTrace();
    }
  }

  // Initialize the server and start listening.
  public void main() throws Exception {
    Selector s = connect(env.get("host"), env.getInt("port"));

    populateModules();

    initThreadPool();

    // Select sockets from selector in a loop.
    while (s.select() >= 0) {
      Iterator<SelectionKey> it = s.selectedKeys().iterator();
      while (it.hasNext()) try {
        SelectionKey k = it.next();
        if (k.isValid()) {
          System.out.println("Key: "+k.readyOps()+" "+k.channel());
          int ops = k.readyOps();
          if ((ops & k.OP_ACCEPT) > 0)
            acceptClient(k);
          if ((ops & k.OP_READ) > 0)
            readCommand(k);
          if ((ops & k.OP_WRITE) > 0)
            writeResponse(k);
        } it.remove();
      } catch (Exception e) {
        System.out.println("Warning: badness in select loop");
        e.printStackTrace();
      }
    }
  }

  // The actual entry point for the server. Includes exception handler
  // for debugging output.
  public void run() {
    try {
      main();
    } catch (Exception e) {
      System.err.println("Error: "+e.getMessage());
      e.printStackTrace();
    }
  }

  public StorkServer(Ad env) {
    this.env = (env != null) ? env : new Ad();

    // Initialize command handlers
    cmd_handlers = new HashMap<String, StorkCommand>();
    cmd_handlers.put("stork_q", new StorkQHandler());
    cmd_handlers.put("stork_ls", new StorkListHandler());
    cmd_handlers.put("stork_status", new StorkQHandler());
    cmd_handlers.put("stork_submit", new StorkSubmitHandler());
    cmd_handlers.put("stork_rm", new StorkRmHandler());
    cmd_handlers.put("stork_info", new StorkInfoHandler());

    // Initialize transfer module set
    xfer_modules = new TransferModuleTable();

    // Initialize queues
    job_queue = new LinkedBlockingQueue<StorkJob>();
    req_queue = new LinkedBlockingQueue<ClientRequest>();
    all_jobs = new ArrayList<StorkJob>();
  }
}
