package stork.core.handlers;

import stork.core.server.*;
import stork.feather.*;
import stork.scheduler.*;

/** Handles scheduling jobs. */
public class SubmitHandler extends Handler<JobRequest> {
  public void handle(JobRequest req) {
    req.assertLoggedIn();
    req.assertMayChangeState();

    req.validate();

    Job job = req.user().createJob(req);

    System.out.println(req.server.scheduler.add(job));
    if (req.server.scheduler.add(job)) {
      req.user().saveJob(job);
      server.dumpState();
    }

    req.ring(job);
  }
}

class JobRequest extends Request {
  private SubEndpointRequest src, dest;

  // Hack to get around marshalling limitations.
  private class SubEndpointRequest extends EndpointRequest {
    public Server server() { return user().server(); }
    public User user() { return JobRequest.this.user(); }
  };

  // TODO: More validations.
  public JobRequest validate() {
    src.validateAs("source");
    dest.validateAs("destination");
    return this;
  }
}
