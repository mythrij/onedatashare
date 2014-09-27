package stork.core.handlers;

import java.util.*;

import stork.ad.*;
import stork.core.server.*;
import stork.scheduler.*;

public class QHandler extends Handler<QRequest> {
  public void handle(QRequest req) {
    req.assertLoggedIn();
    //final List list = new JobSearcher(req.user.jobs).query(Ad.marshal(req));
    final List list = req.user().jobs;
    req.ring(req.count ? new Object() { int count = list.size(); } : list);
  }
}

class QRequest extends Request {
  boolean count = false;
}
