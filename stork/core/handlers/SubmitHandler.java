package stork.core.handlers;

import stork.core.server.*;
import stork.feather.*;
import stork.scheduler.*;

/** Handles scheduling jobs. */
public class SubmitHandler extends Handler<JobRequest> {
  public void handle(JobRequest req) {
    req.assertLoggedIn();
    req.assertMayChangeState();

    Job job = req.toJob();

    // Schedule the job to execute and add the job to the user context.
    req.server.scheduler.add(job);

    synchronized (req.user) {
      req.user.jobs.add(job);
      job.jobId(req.user.jobs.size());
    }

    req.ring(job);
  }
}

class JobRequest extends Request {
  private SubEndpointRequest src, dest;

  // Kind of a hack...
  private class SubEndpointRequest extends EndpointRequest {{
    server = JobRequest.this.server;
    user = JobRequest.this.user;
  }};

  public Job toJob() { return null; }
}
