package stork.scheduler;

import stork.ad.*;
import stork.util.*;
import static stork.scheduler.JobStatus.*;

import java.util.*;

// A blocking queue for Stork jobs. Allows for scheduling of jobs and
// searching of past jobs.

public class JobQueue {
  private transient LinkedList<StorkJob> waiting;
  private ArrayList<StorkJob> all;

  // Initialize all the queues and stuff.
  public JobQueue() {
    waiting = new LinkedList<StorkJob>();
    all = new ArrayList<StorkJob>();
  }

  // Ugh, I wish we didn't have to do this, but ad deserialization is kind
  // of bad at collection subclasses.
  public static JobQueue unmarshal(Ad ad) {
    JobQueue jq = new JobQueue();
    
    for (Ad a : ad.getAds("all")) {
      StorkJob j = a.unmarshalAs(StorkJob.class);
      if (!j.isComplete())
        j.status(scheduled);
      jq.put(j);
    } return jq;
  }

  // Take a waiting job from the queue, blocking until one is available.
  public synchronized StorkJob take() {
    while (waiting.isEmpty()) try {
      wait();
    } catch (Exception e) {
      onInterrupt();
    } return waiting.pop();
  }

  // Put a new job into the queue.
  public synchronized StorkJob put(StorkJob job) {
    return put(job, false);
  } public synchronized StorkJob put(StorkJob job, boolean first) {
    if (job.status() == JobStatus.scheduled)
      schedule(job);

    if (job.jobId() <= 0) {
      all.add(job);
      job.jobId(all.size());
    } else {
      all.ensureCapacity(job.jobId());
      all.add(job.jobId()-1, job);
    } notifyAll();

    return job;
  }

  // Schedule a job to execute.
  public synchronized void schedule(StorkJob job) {
    schedule(job, false);
  } public synchronized void schedule(StorkJob job, boolean first) {
    if (first) waiting.push(job);
    else       waiting.add(job);
  }

  // Get a job by its id.
  public synchronized StorkJob get(int job_id) {
    try {
      return all.get(job_id-1);
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
  public List<StorkJob> get(Ad ad) {
    List<StorkJob> list = new LinkedList<StorkJob>();

    // Filter fields.
    String user_id = null;
    Range range = null;
    EnumSet<JobStatus> status = JobStatus.pending.filter();

    // Parse fields from input ad.
    if (ad != null) {
      user_id = ad.get("user_id");
      if (ad.has("range"))
        range = Range.parseRange(ad.get("range"));
      if (ad.has("status"))
        status = JobStatus.byName(ad.get("status")).filter();
      else if (range != null)
        status = JobStatus.all.filter();
      if (range == null)
        range = new Range(1, all.size());
    }

    // Perform a simple but not very efficient O(n) selection.
    for (int i : range) {
      StorkJob j = get(i);
      if (j != null)
      if (status.contains(j.status()))
      if (user_id == null || user_id.equals(j.user_id()))
        list.add(j);
    } return list;
  }

  // Perform a query and return a list of ads instead of jobs.
  public void getAds(Ad ad, AdSorter sorter) {
    for (StorkJob j : get(ad))
      sorter.add(j.getAd());
  }

  // Do this whenever a wait is interrupted. Could potentially throw
  // an exception, but for now just do nothing because we don't
  // interrupt any of our threads.
  public void onInterrupt() { }
}
