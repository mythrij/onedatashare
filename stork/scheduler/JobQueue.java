package stork.scheduler;

import stork.ad.*;
import stork.util.*;

import java.util.*;

// A blocking queue for Stork jobs. Allows for scheduling of jobs and
// searching of past jobs.

public class JobQueue {
  private ArrayList<StorkJob> all_jobs;
  private transient LinkedList<StorkJob> new_jobs;

  // Initialize all the queues and stuff.
  public JobQueue() {
    all_jobs = new ArrayList<StorkJob>();
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
  public synchronized StorkJob add(StorkJob job) {
    return add(job, false);
  } public synchronized StorkJob add(StorkJob job, boolean first) {
    if (job.status() == JobStatus.scheduled)
      schedule(job);

    if (job.jobId() == -1) {
      all_jobs.add(job);
      job.jobId(all_jobs.size());
    } else {
      all_jobs.add(job.jobId(), job);
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
      return all_jobs.get(job_id-1);
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
  public List<Ad> get() {
    return get(null);
  } public List<Ad> get(Ad ad) {
    List<Ad> list = new LinkedList<Ad>();

    // Filter fields.
    String user_id = null;
    Range range = new Range(1, all_jobs.size());
    EnumSet<JobStatus> status = JobStatus.all.filter();

    // Parse fields from input ad. These all return null if passed null.
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
        list.add(j.getAd());
    } return list;
  }

  // Do this whenever a wait is interrupted. Could potentially throw
  // an exception, but for now just do nothing because we don't
  // interrupt any of our threads.
  public void onInterrupt() { }
}
