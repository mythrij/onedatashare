package stork.scheduler;

import stork.ad.*;
import stork.util.*;
import stork.module.*;
import static stork.scheduler.JobStatus.*;

import java.net.URI;

// A representation of a transfer job submitted to Stork. The entire
// state of the job should be stored in the ad representing this job.
// This includes the start time

public class StorkJob extends Ad {
  private JobStatus status = null;
  private EndPoint src, dest;
  private String x509_proxy = null;

  Watch queue_timer = new Watch(), run_timer = new Watch();

  // Create a job from a submit ad.
  public StorkJob(Ad ad) {
    super(false, ad);
    //filter("src", "dest", "module", "cred_token", "max_attempts");

    // Check for required src and dest.
    String req = ad.require("src", "dest");
    if (req != null)
      throw new FatalEx("missing "+req+" field");

    // Don't allow chained ads for now.
    if (ad.hasNext())
      throw new FatalEx("chained ads are not allowed");

    // Pull this out if it's there.
    x509_proxy = ad.get("x509_proxy");
    try {
      remove("x509_proxy");
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }

    // Parse them into URIs.
    src = getEndPoint("src");
    dest = getEndPoint("dest");
    
    status(scheduled);
  }

  // Gets the job info as an ad.
  public synchronized Ad getAd() {
    return new Ad(this);
  }

  // Determine either the src or dest end-point. Checks the 
  private EndPoint getEndPoint(String w) {
    Ad ea = new Ad("module", get("module"))
              .put("cred_token", get("cred_token"))
              .put("x509_proxy", x509_proxy);
    Object o = getObject(w);
    if (o instanceof Ad)
      return new EndPoint(ea.merge((Ad)o));
    if (o instanceof String)
      return new EndPoint(ea.put("uri", (String)o));
    throw new FatalEx(w+" is not a string or ad");
  }

  // Sets the status of the job, updates ad, and adjusts state
  // according to the status.
  public synchronized JobStatus status() {
    return status;
  } public synchronized void status(JobStatus s) {
    assert !s.isFilter;

    status = s;
    put("status", s.name());

    // Update state.
    switch (s) {
      case scheduled:
        queue_timer.start(); break;
      case processing:
        run_timer.start(); break;
      case removed:
      case failed:
      case complete:
        queue_timer.stop();
        run_timer.stop();
    }
  }

  // Get/set the job id.
  public synchronized int jobId() {
    return getInt("job_id");
  } public synchronized void jobId(int id) {
    put("job_id", id);
  }

  // Called when the job gets removed. Returns true if the job had its
  // state updated, and false otherwise (e.g., the job was already
  // complete or couldn't be removed).
  public synchronized boolean removeJob(String reason) {
    switch (status) {
      // Try to stop the job. If we can't, don't do anything.
      case processing:
        run_timer.stop();

      // Fall through to set removed status.
      case scheduled:
        put("message", reason);
        status(JobStatus.removed);
        return true;

      // In any other case, the job has ended, do nothing.
      default:
        return false;
    }
  }

  // Check if the job should be rescheduled.
  public synchronized boolean shouldReschedule() {
    int attempts = getInt("attempts", 0);
    int max = getInt("max_attempts", 10);

    // If we've failed, don't reschedule.
    if (status == failed)
      return false;

    // Check for custom max attempts.
    if (max > 0 && attempts >= max)
      return false;

    // Check for configured max attempts.
    //max = env.getInt("max_attempts", 10);
    //if (max > 0 && attempts >= max)
      //return false;

    return true;
  }

  // Run the job and return the status.
  public JobStatus process() {
    run();
    return status;
  }

  // Run the job and watch it to completion.
  // FIXME: Race condition?
  public void run() {
    StorkSession ss = null, ds = null;

    try {
      synchronized (this) {
        // Must be scheduled to be able to run.
        if (status != JobStatus.scheduled)
          throw new FatalEx("trying to run unscheduled job");
        status(processing);
      }

      // Establish connections to end-points.
      ss = src.session();
      ds = dest.session();
      ss.pair(ds);

      // Create a pipe to process progress ads from the module.
      Pipe<Ad> pipe = new Pipe<Ad>();
      pipe.new End(false) {
        void store(Ad ad) {
          System.out.println("Got aux ad: "+ad);
          merge(ad);
        }
      };

      // Begin the transfer after attaching pipe.
      ss.setPipe(pipe);
      ss.transfer(src.path, dest.path);

      // We made it!
      status(complete);
    } catch (Exception e) {
      if (e instanceof FatalEx || !shouldReschedule()) {
        status(failed);
      } else {
        status(scheduled);
        put("attempts", getInt("attempts", 0)+1);
      } put("message", e.getMessage());
      e.printStackTrace();
    } finally {
      if (ss != null) ss.close();
      if (ds != null) ds.close();
    }
  }
}
