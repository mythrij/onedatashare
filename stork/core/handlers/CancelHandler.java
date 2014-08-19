package stork.core.handlers;

import java.util.*;

import stork.ad.*;
import stork.core.server.*;
import stork.scheduler.*;
import stork.util.*;

/** Handle removal of a job. */
public class CancelHandler extends Handler<CancelRequest> {
  public void handle(CancelRequest req) {
    req.assertLoggedIn();
    req.assertMayChangeState();

    Range sdr = new Range(), cdr = new Range();

    if (req.range.isEmpty())
      throw new RuntimeException("No jobs specified.");

    // Find ad in job list, set it as removed.
    List<Job> list = new JobSearcher(req.user.jobs).query(Ad.marshal(req));
    for (Job job : list) try {
      job.remove("Removed by user.");
      sdr.swallow(job.jobId());
    } catch (Exception e) {
      Log.info("Couldn't remove job ", job.jobId(), ": ", e.getMessage());
      cdr.swallow(job.jobId());
    }

    // See if there's anything in our "couldn't delete" range.
    if (sdr.isEmpty())
      throw new RuntimeException("No jobs were removed.");
    Ad ad = new Ad("removed", sdr.toString());
    if (!cdr.isEmpty())
      ad.put("not_removed", cdr.toString());
    req.ring(ad);
  }
}

class CancelRequest extends Request {
  Range range;
}
