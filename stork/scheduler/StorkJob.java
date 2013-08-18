package stork.scheduler;

import stork.ad.*;
import stork.util.*;
import stork.module.*;
import static stork.scheduler.JobStatus.*;

import java.net.URI;

// A representation of a transfer job submitted to Stork. The entire
// state of the job should be stored in the ad representing this job.
//
// As transfers progress, update ads will be sent by the underlying
// transfer code.
//
// The following fields can come from the transfer module in an update ad:
//   bytes_total - the number of bytes to transfer
//   files_total - the number of files to transfer
//   bytes_done  - indication that some bytes have been transferred
//   files_done  - indication that some files have been transferred
//   complete    - true if success, false if failure

public class StorkJob {
  private int job_id = 0;
  private JobStatus status = null;
  private EndPoint src = null, dest = null;
  private String x509_proxy = null;
  private String user_id = null;
  private transient TransferProgress tp = new TransferProgress();

  private int attempts = 0, max_attempts = 10;
  private String message = null;

  private transient Watch queue_timer = new Watch(),
                          run_timer   = new Watch();

  // Create and enqueue a new job from a user input ad.
  // TODO: Strict filtering and checking.
  public static StorkJob create(Ad ad) {
    ad.remove("status", "job_id", "attempts", "user_id");
    ad.rename("src_url",  "src.url");
    ad.rename("dest_url", "dest.url");

    System.out.println(ad);
    StorkJob j = ad.unmarshalAs(StorkJob.class).status(scheduled);

    if (j.src == null || j.dest == null)
      throw new RuntimeException("src or dest was null");
    return j;
  }

  // Gets the job info as an ad, merged with progress ad.
  // TODO: More proper filtering.
  public synchronized Ad getAd() {
    return Ad.marshal(this).merge(tp.getAd()).remove("x509_proxy");
  }

  // Get the user_id of the user who owns this job.
  public String user_id() {
    return user_id;
  }

  // Sets the status of the job, updates ad, and adjusts state
  // according to the status.
  public synchronized JobStatus status() {
    return status;
  } public synchronized StorkJob status(JobStatus s) {
    assert !s.isFilter;

    status = s;

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
    } return this;
  }

  // Get/set the job id.
  public synchronized int jobId() {
    return job_id;
  } public synchronized void jobId(int id) {
    job_id = id;
  }

  // Called when the job gets removed. Returns true if the job had its
  // state updated, and false otherwise (e.g., the job was already
  // complete or couldn't be removed).
  public synchronized boolean remove(String reason) {
    switch (status) {
      // Try to stop the job. If we can't, don't do anything.
      case processing:
        run_timer.stop();

      // Fall through to set removed status.
      case scheduled:
        message = reason;
        status(JobStatus.removed);
        return true;

      // In any other case, the job has ended, do nothing.
      default:
        return false;
    }
  }

  // Check if the job should be rescheduled.
  public synchronized boolean shouldReschedule() {
    // If we've failed, don't reschedule.
    if (status == failed)
      return false;

    // Check for custom max attempts.
    if (max_attempts > 0 && attempts >= max_attempts)
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
        public void store(Ad ad) {
          if (ad.has("bytes_total") || ad.has("files_total"))
            tp.transferStarted(ad.getLong("bytes_total"),
                               ad.getInt("files_total"));
          if (ad.has("bytes_done") || ad.has("files_done"))
            tp.done(ad.getLong("bytes_done"), ad.getInt("files_done"));
          if (ad.getBoolean("complete"))
            tp.transferEnded(ad.get("error") == null);
        }
      };

      // Begin the transfer after attaching pipe.
      pipe.new End().put(new Ad());
      ss.setPipe(pipe.new End());
      ss.transfer(src.path(), dest.path());

      // We made it!
      status(complete);
    } catch (Exception e) {
      // Tell the transfer progress we're done.
      tp.transferEnded(false);

      if (e instanceof FatalEx || !shouldReschedule()) {
        status(failed);
      } else {
        status(scheduled);
        attempts++;
      } message = e.getMessage();
    } finally {
      if (ss != null) ss.close();
      if (ds != null) ds.close();
    }
  }
}
