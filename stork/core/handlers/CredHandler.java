package stork.core.handlers;

import java.util.*;

import stork.core.server.*;
import stork.cred.*;

/** Handles creating credentials. */
public class CredHandler extends Handler<CredRequest> {
  public void handle(CredRequest req) {
    req.assertLoggedIn();
    req.assertMayChangeState();

    if (req.action == null) {
      throw new RuntimeException("No action specified.");
    } if (req.action.equals("create")) {
      StorkCred<?> cred = req.resolve();
      String uuid = UUID.randomUUID().toString();
      req.user.credentials.put(uuid, cred);
      req.ring(new stork.ad.Ad("uuid", uuid));
    } throw new RuntimeException("Invalid action.");
  }
}

class CredRequest extends Request {
  String action;
  String type;

  public StorkCred resolve() {
    StorkCred cred = StorkCred.newFromType(type);
    asAd().unmarshal(cred);
    return cred;
  }
}
