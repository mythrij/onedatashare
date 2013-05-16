package stork.scheduler;

import stork.ad.*;
import stork.util.*;

import java.util.*;

// A blocking queue for Stork jobs. Allows for scheduling of jobs and
// searching of past jobs.
// TODO: Serializability.

public class JobQueue extends Ad {
  private LinkedList<StorkJob> new_jobs;
  private ArrayList<StorkJob> all_jobs;

  public JobQueue() {
    new_jobs = new LinkedList<StorkJob>();
    all_jobs = new ArrayList<StorkJob>();
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
  public synchronized void add(StorkJob job) {
    add(job, false);
  } public synchronized void add(StorkJob job, boolean first) {
    if (job.status() == JobStatus.scheduled) {
      if (first) new_jobs.push(job);
      else       new_jobs.add(job);
    }

    if (job.jobId() == -1) {
      all_jobs.add(job);
      job.jobId(all_jobs.size());
    } else {
      all_jobs.add(job.jobId(), job);
    } notifyAll();
  }

  // Get a job by its id.
  public synchronized StorkJob get(int job_id) {
    try {
      return all_jobs.get(job_id-1);
    } catch (Exception e) {
      return null;
    }
  }

  // Search jobs by job id range and/or filter. Either can be null.
  public List<StorkJob> get(String range, String status) {
    return get((range  != null) ? Range.parseRange(range)  : null,
               (status != null) ? JobStatus.byName(status) : null);
  } public List<StorkJob> get(Range range, JobStatus status) {
    List<StorkJob> list = new LinkedList<StorkJob>();

    if (status == null && range != null)
      status = JobStatus.pending;
    else if (status == JobStatus.all)
      status = null;

    if (status != null) {
      EnumSet<JobStatus> filter = status.filter();
      if (range != null) for (int i : range) {
        StorkJob j = get(i);
        if (j != null && filter.contains(j.status())) list.add(j);
      } else for (int i = 1; i <= all_jobs.size(); i++) {
        StorkJob j = get(i);
        if (j != null && filter.contains(j.status())) list.add(j);
      }
    } else {
      if (range != null) for (int i : range) {
        StorkJob j = get(i);
        if (j != null) list.add(j);
      } else for (int i = 1; i <= all_jobs.size(); i++) {
        StorkJob j = get(i);
        if (j != null) list.add(j);
      }
    } return list;
  }

  // Do this whenever a wait is interrupted. Could potentially throw
  // and exception, but for now just do nothing because we don't
  // interrupt any of our threads.
  public void onInterrupt() { }
}
