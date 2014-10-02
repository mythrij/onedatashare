package stork.scheduler;

import java.util.LinkedList;

public class FIFOScheduler extends Scheduler {

	private LinkedList<Job> _scheduledJobs;

	private int _runningJobs;

	public FIFOScheduler() {
		// TODO Auto-generated constructor stub
		_scheduledJobs = new LinkedList<Job>();

		_runningJobs = 0;
	}

	@Override
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
		if (stork.core.Config.loadConfig().max_jobs == 0) {
			return true;
		} else if (getRunningJobs() < stork.core.Config.loadConfig().max_jobs) {
			return true;
		} else {
			return false;
		}
	}

	private void runJob(final Job job) {
		job.start().new Promise() {
			private int _currentAttempt = 0;

			@Override
			protected void done() throws Throwable {
				// TODO Auto-generated method stub
				switch (job.status()) {
				case complete:
					// check if the queue is empty. If not empty, run next job
					// if it is empty, release an open spot
					if (!_scheduledJobs.isEmpty()) {
						runJob(_scheduledJobs.poll());
					} else {
						decrementRunningJobs();
					}
					break;
				case failed:
					if (_currentAttempt < stork.core.Config.loadConfig().max_attempts) {
						_currentAttempt++;
						job.status(JobStatus.scheduled);
						job.start().promise(this);
					} else {
						// do something if max attempts reached
						job.status(JobStatus.failed, "Max Attempts Failed");
					}
					break;
				default:
					System.err.println("Unexpected status:" + job.status());
					break;
				}
			}
		};
	}
	
	synchronized private int getRunningJobs(){ return _runningJobs; }
	synchronized private void incrementRunningJobs(){ _runningJobs++; };
	synchronized private void decrementRunningJobs(){ _runningJobs--; };

	@Override
	public Job get(int id) {
		// TODO Auto-generated method stub
		for (Job j : _scheduledJobs) {
			if (j.getID() == id) {
				return j;
			}
		}

		return null;
	}

}
