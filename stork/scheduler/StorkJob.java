package stork.scheduler;

import stork.*;
import stork.ad.*;
import stork.util.*;
import stork.module.*;
import stork.user.*;
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
  private JobStatus status;
  private EndPoint src, dest;
  private TransferProgress progress = new TransferProgress();

  private int attempts = 0, max_attempts = 10;
  private String message;

  private Ad options;

  private Watch queue_timer;
  private Watch run_timer;

  private transient User user;
  private transient Thread thread;

  // Create and enqueue a new job from a user input ad. Don't give this
  // thing unsanitized user input, because it doesn't filter the user_id.
  // That should be filtered by the caller.
  // TODO: Strict filtering and checking.
  public static StorkJob create(User user, Ad ad) {
    ad.remove("status", "job_id", "attempts");
    ad.rename("src_url",  "src.url");
    ad.rename("dest_url", "dest.url");

    StorkJob j = ad.unmarshalAs(StorkJob.class).status(scheduled);
    j.user = user;

    if (j.src == null || j.dest == null)
      throw new RuntimeException("src or dest was null");
    return j;
  }

  // Gets the job info as an ad, merged with progress ad.
  // TODO: More proper filtering.
  public synchronized Ad getAd() {
    return Ad.marshal(this);
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
        progress.transferEnded(true);
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
      throw new RuntimeException("The job has already terminated.");
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
    int max = Stork.settings.max_attempts;
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
  public void run() {
    StorkSession ss = null, ds = null;

    try {
      synchronized (this) {
        // Must be scheduled to be able to run.
        if (status != scheduled)
          throw new RuntimeException("trying to run unscheduled job");
        status(processing);
        thread = Thread.currentThread();
      }

      // Establish connections to end-points.
      ss = src.session();
      ds = dest.session();

      // If options were given, marshal them into the sessions.
      if (options != null) {
        System.out.println("Marshalling options: "+options);
        options.unmarshal(ss);
        options.unmarshal(ds);
        System.out.println(Ad.marshal(ss));
        System.out.println(Ad.marshal(ds));
      }

      // Create a pipe to process progress ads from the module.
      Pipe<Ad> pipe = new Pipe<Ad>();
      pipe.new End(false) {
        public void store(Ad ad) {
          Log.finer("Progress: ", ad);
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
      ss.setPipe(pipe.new End());

      // Get the file trees. FIXME: hax
      Bell<FileTree> dfb = ds.list(dest.path());
      FileTree sft = ss.list(src.path(), new Ad("recursive", true)).waitFor();
      FileTree dft;

      try {
        dft = dfb.waitFor();
      } catch (Exception e) {
        // Assume it's a new whatever the source is.
        dft = new FileTree(sft.name);
        dft.copy(sft);
      }

      if (sft.dir && dft.file)
        throw new RuntimeException("cannot transfer from directory to file");

      // Open the files on the endpoints.
      StorkChannel sc = ss.open(StorkUtil.dirname(src.path()),  sft);
      StorkChannel dc = (dft.dir) ? ds.open(dest.path(), sft)
                                  : ds.open(StorkUtil.dirname(dest.path()), dft);

      // If the destination exists and overwriting is not enabled, fail.
      if (!ds.overwrite && dc.exists())
        throw new RuntimeException("Destination file already exists.");

      // Let the transfer module do the rest.
      sc.sendTo(dc).waitFor();

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
      } else if (!isTerminated()) {
        status(failed);
        message = e.getMessage();
      } e.printStackTrace();
    } finally {
      // Any time we're not running, the sessions should be closed.
      thread = null;
      if (ss != null) ss.close();
      if (ds != null) ds.close();
    }
  }
}
