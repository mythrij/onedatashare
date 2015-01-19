package stork.core.handlers;

import stork.core.server.*;
import stork.feather.*;

/** Handles retrieving files. */
public class GetHandler extends Handler<EndpointRequest> {
  public void handle(final EndpointRequest req) {
    req.assertLoggedIn();

    final Resource resource = req.server.sessions.take(req.resolve());
    Transfer t = resource.transferTo(req.resource);
    t.start();
    t.onStop().new Promise() {
      public void done() {
        req.ring();
      } public void fail(Throwable t) {
        req.ring(t);
      } public void always() {
        server.sessions.put(resource.session);
      }
    };
  }
}
