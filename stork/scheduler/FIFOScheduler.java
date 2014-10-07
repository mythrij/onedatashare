package stork.scheduler;

import java.util.LinkedList;

public class FIFOScheduler extends Scheduler {

  private LinkedList<Job> _scheduledJobs;

  private int _runningJobs;

  public FIFOScheduler() {
    _scheduledJobs = new LinkedList<Job>();

    _runningJobs = 0;
  }

  protected void schedule(Job job) {
    // start the job
    if (canRunJob()) {
      runJob(job);
      incrementRunningJobs();
    } else {
      _scheduledJobs.add(job);
    }
  }

  private boolean canRunJob() {
    if (server().config.max_jobs == 0) {
      return true;
    } else if (getRunningJobs() < server().config.max_jobs) {
      return true;
    } else {
      return false;
    }
  }

  private void runJob(final Job job) {
    job.start().new Promise() {
      protected void done() throws Throwable {
        if (!_scheduledJobs.isEmpty()) {
          runJob(_scheduledJobs.poll());
        } else {
          decrementRunningJobs();
        }
      }
    };
  }
  
  synchronized private int getRunningJobs(){ return _runningJobs; }
  synchronized private void incrementRunningJobs(){ _runningJobs++; };
  synchronized private void decrementRunningJobs(){ _runningJobs--; };
}
