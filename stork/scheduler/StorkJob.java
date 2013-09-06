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

@SuppressWarnings("fallthrough")
public class StorkJob {
  private int job_id = 0;
  private JobStatus status = null;
  private EndPoint src = null, dest = null;
  private String user_id = null;
  private TransferProgress progress = new TransferProgress();

  private int attempts = 0, max_attempts = 10;
  private String message = null;

  private Watch queue_timer = null;
  private Watch run_timer   = null;

  private transient Thread thread = null;
  private transient StorkScheduler sched = null;

  // Create and enqueue a new job from a user input ad. Don't give this
  // thing unsanitized user input, because it doesn't filter the user_id.
  // That should be filtered by the caller.
  // TODO: Strict filtering and checking.
  public static StorkJob create(Ad ad) {
    ad.remove("status", "job_id", "attempts");
    ad.rename("src_url",  "src.url");
    ad.rename("dest_url", "dest.url");

    StorkJob j = ad.unmarshalAs(StorkJob.class).status(scheduled);

    if (j.src == null || j.dest == null)
      throw new RuntimeException("src or dest was null");
    return j;
  }

  // Call this before scheduling and before executing.
  public StorkJob scheduler(StorkScheduler s) {
    src.scheduler(sched = s);
    dest.scheduler(s);
    return this;
  }

  // Gets the job info as an ad, merged with progress ad.
  // TODO: More proper filtering.
  public synchronized Ad getAd() {
    return Ad.marshal(this);
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

    // Update state.
    switch (status = s) {
      case scheduled:
        queue_timer = new Watch(true); break;
      case processing:
        run_timer = new Watch(true); break;
      case removed:
        if (thread != null)
          thread.interrupt();
      case failed:
      case complete:
        queue_timer.stop();
        run_timer.stop();
        progress.transferEnded(false);
    } return this;
  }

  // Get/set the job id.
  public synchronized int jobId() {
    return job_id;
  } public synchronized void jobId(int id) {
    job_id = id;
  }

  // Called when the job gets removed from the queue.
  public synchronized void remove(String reason) {
    if (isTerminated())
      throw new RuntimeException("job cannot be removed");
    message = reason;
    status(removed);
  }

  // This will increment the attempts counter, and set the status
  // back to scheduled. It does not actually put the job back into
  // the scheduler queue.
  public synchronized void reschedule() {
    attempts++;
    status(scheduled);
  }

  // Check if the job should be rescheduled.
  public synchronized boolean shouldReschedule() {
    // If we've failed, don't reschedule.
    if (isTerminated())
      return false;

    // Check for custom max attempts.
    if (max_attempts > 0 && attempts >= max_attempts)
      return false;

    // Check for configured max attempts.
    int max = sched.env.getInt("max_attempts");
    if (max > 0 && attempts >= max)
      return false;

    return true;
  }

  // Run the job and return the status.
  public JobStatus process() {
    run();
    return status;
  }

  // Return whether or not the job has terminated.
  public boolean isTerminated() {
    switch (status) {
      case scheduled:
      case processing:
      case paused:
        return false;
      default:
        return true;
    }
  }

  // Run the job and watch it to completion.
  // FIXME: Race condition?
  public void run() {
    StorkSession session = null;

    try {
      synchronized (this) {
        // Must be scheduled to be able to run.
        if (status != scheduled)
          throw new RuntimeException("trying to run unscheduled job");
        status(processing);
        thread = Thread.currentThread();
      }

      // Establish connections to end-points.
      session = src.pairWith(dest);

      // Create a pipe to process progress ads from the module.
      Pipe<Ad> pipe = new Pipe<Ad>();
      pipe.new End(false) {
        public void store(Ad ad) {
          if (ad.has("bytes_total") || ad.has("files_total"))
            progress.transferStarted(ad.getLong("bytes_total"),
                               ad.getInt("files_total"));
          if (ad.has("bytes_done") || ad.has("files_done"))
            progress.done(ad.getLong("bytes_done"), ad.getInt("files_done"));
          if (ad.getBoolean("complete"))
            progress.transferEnded(!ad.has("error"));
        }
      };

      // Set the pipe.
      pipe.new End().put(new Ad());
      session.setPipe(pipe.new End());

      // Now let the transfer module do the rest.
      session.transfer(src.path(), dest.path());

      // No exceptions happened. We did it!
      status(complete);
    } catch (ModuleException e) {
      if (e.isFatal() || !shouldReschedule())
        status(failed);
      else
        reschedule();
      message = e.getMessage();
    } catch (Exception e) {
      // Only change the state if the exception isn't from an interrupt.
      if (e != Pipeline.PIPELINE_ABORTED) {
        status(failed);
        message = e.getMessage();
      }
    } finally {
      // Any time we're not running, the sessions should be closed.
      thread = null;
      if (session != null)
        session.closeBoth();
    }
  }
}
