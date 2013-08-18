package stork.scheduler;

import stork.ad.*;
import stork.util.*;

import java.util.*;

// A blocking queue for Stork jobs. Allows for scheduling of jobs and
// searching of past jobs.

public class JobQueue extends ArrayList<StorkJob> {
  private transient LinkedList<StorkJob> new_jobs;

  // Initialize all the queues and stuff.
  public JobQueue() {
    new_jobs = new LinkedList<StorkJob>();
  }

  // Take a new job from the queue, blocking until one is available.
  public synchronized StorkJob take() {
    while (new_jobs.isEmpty()) try {
      wait();
    } catch (Exception e) {
      onInterrupt();
    } return new_jobs.pop();
  }

  // Put a new job into the queue.
  public synchronized StorkJob put(StorkJob job) {
    return put(job, false);
  } public synchronized StorkJob put(StorkJob job, boolean first) {
    if (job.status() == JobStatus.scheduled)
      schedule(job);

    if (job.jobId() == -1) {
      super.add(job);
      job.jobId(super.size());
    } else {
      add(job.jobId(), job);
    } notifyAll();

    return job;
  }

  // Schedule a job to execute.
  public synchronized void schedule(StorkJob job) {
    schedule(job, false);
  } public synchronized void schedule(StorkJob job, boolean first) {
    if (first) new_jobs.push(job);
    else       new_jobs.add(job);
  }

  // Get a job by its id.
  public synchronized StorkJob get(int job_id) {
    try {
      return super.get(job_id-1);
    } catch (Exception e) {
      return null;
    }
  }

  // Search jobs using an optional filter ad. The filter may contain the
  // following fields:
  //   range - a range of job ids to select
  //   status - the name of a job status filter
  //   user_id - the user to select for
  // The results are returned as a list.
  public Ad get(Ad ad) {
    Ad list = new Ad();

    // Filter fields.
    String user_id = null;
    Range range = new Range(1, super.size());
    EnumSet<JobStatus> status = JobStatus.all.filter();

    // Parse fields from input ad.
    if (ad != null) {
      user_id = ad.get("user_id");
      if (ad.has("range"))
        range = Range.parseRange(ad.get("range"));
      if (ad.has("status"))
        status = JobStatus.byName(ad.get("status")).filter();
    }

    // Perform a simple but not very efficient O(n) selection.
    for (int i : range) {
      StorkJob j = get(i);
      if (j != null)
      if (status.contains(j.status()))
      if (user_id == null || user_id.equals(j.user_id()))
        list.put(j.getAd());
    } return list;
  }

  // Do this whenever a wait is interrupted. Could potentially throw
  // an exception, but for now just do nothing because we don't
  // interrupt any of our threads.
  public void onInterrupt() { }
}
