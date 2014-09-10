package stork.scheduler;

import java.util.*;
import java.util.concurrent.*;

import stork.ad.*;
import stork.core.*;
import stork.core.server.*;
import stork.core.handlers.*;
import stork.feather.*;

import static stork.scheduler.JobStatus.*;

/** A transfer job. */
public abstract class Job {
  private int job_id = 0;
  private JobStatus status;
  private JobEndpointRequest src, dest;
  private int attempts = 0, max_attempts = 10;
  private String message;

  private Map<?,?> options;

  private transient Transfer transfer;

  private class JobEndpointRequest extends EndpointRequest {
    public JobEndpointRequest() {
      System.out.println("MADE");
      new Error().printStackTrace();
    }
    public Server server() { return Job.this.user().server(); }
    public User user() {
      System.out.println(getClass());
      return Job.this.user();
    }
  }

  /** The {@code User} this {@code Job} belongs to. */
  public abstract User user();

  /**
   * Sets the status of the job, updates ad, and adjusts state according to the
   * status. This can also be used to set a message.
   */
  public synchronized JobStatus status() {
    return status;
  } public synchronized Job status(JobStatus s) {
    return status(s, message);
  } public synchronized Job status(String msg) {
    return status(status, msg);
  } public synchronized Job status(JobStatus s, String msg) {
    assert !s.isFilter;

    message = msg;

    // Update state.
    switch (status = s) {
      case scheduled:
        /*queue_timer = new Watch(true);*/ break;
      case processing:
        /*run_timer = new Watch(true);*/ break;
      case removed:
        if (transfer != null)
          transfer.cancel();
      case failed:
      case complete:
        //queue_timer.stop();
        //run_timer.stop();
        //progress.transferEnded(true);
    } return this;
  }

  /** Get the job id. */
  public synchronized int jobId() {
    return job_id;
  }

  /** Set the job id. */
  public synchronized void jobId(int id) {
    job_id = id;
  }

  /** Called when the job gets removed from the queue. */
  public synchronized void remove(String reason) {
    if (isTerminated())
      throw new RuntimeException("The job has already terminated.");
    message = reason;
    status(removed);
  }

  /**
   * This will increment the attempts counter, and set the status
   * back to scheduled. It does not actually put the job back into
   * the scheduler queue.
   */
  public synchronized void reschedule() {
    attempts++;
    status(scheduled);
  }

  /** Check if the job should be rescheduled. */
  public synchronized boolean shouldReschedule() {
    // If we've failed, don't reschedule.
    if (isTerminated())
      return false;

    // Check for custom max attempts.
    if (max_attempts > 0 && attempts >= max_attempts)
      return false;

    // Check for configured max attempts.
    int max = Config.global.max_attempts;
    if (max > 0 && attempts >= max)
      return false;

    return true;
  }

  /**
   * Return whether or not the job has terminated.
   */
  public synchronized boolean isTerminated() {
    switch (status) {
      case failed:
      case pending:
      case complete:
        return true;
      default:
        return false;
    }
  }

  /**
   * Start the transfer between the specified resources.
   */
  public synchronized void start() {
    if (status != scheduled) {
      status(failed, "Trying to run unscheduled job.");
    } else {
      status(processing);
      transfer =
        src.resolveAs("source").transferTo(dest.resolveAs("destination"));
      transfer.start();
      transfer.onStop().new Promise() {
        public void done() {
          status(complete);
        } public void fail(Throwable t) {
          status(failed, t.getMessage());
          transfer = null;
        }
      };
    }
  }

  public String toString() {
    return Ad.marshal(this).toString();
  }
}
