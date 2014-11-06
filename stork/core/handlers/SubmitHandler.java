package stork.core.handlers;

import stork.ad.*;
import stork.core.server.*;
import stork.feather.*;
import stork.scheduler.*;

/** Handles scheduling jobs. */
public class SubmitHandler extends Handler<JobRequest> {
  public void handle(JobRequest req) {
    req.assertLoggedIn();
    req.assertMayChangeState();

    req.validate();

    Job job = req.createJob();
    req.server.schedule(job);
    req.user().saveJob(job);

    server.dumpState();

    req.ring(job);
  }
}

class JobRequest extends Request {
  private JobEndpointRequest src, dest;

  // Hack to get around marshalling limitations.
  private class JobEndpointRequest extends EndpointRequest {
    public Server server() { return user().server(); }
    public User user() { return JobRequest.this.user(); }
  };

  // TODO: More validations.
  public JobRequest validate() {
    src.validateAs("source");
    dest.validateAs("destination");
    return this;
  }

  /** Create a {@code Job} from this request. */
  public Job createJob() {
    return Ad.marshal(this).unmarshal(new Job());
  }
}
