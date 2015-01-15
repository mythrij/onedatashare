package stork.core.handlers;

import java.util.*;

import stork.core.server.*;
import stork.cred.*;

/**
 * Create or retrieve a credential. Used to by CredHandler and EndpointRequest.
 */
public class CredRequest extends Request {
  /** An optional name for the credential. */
  String name;
  /** The type of credential being created. */
  String type;
  /** The UUID of the credential being retrieved. */
  UUID uuid;

  // Sigh, this is here as a terrible hack. Please FIXME.
  String myproxy_host;
  int myproxy_port;
  String myproxy_user;
  String myproxy_pass;

  public StorkCred resolve() {
    if (uuid != null) {
      return user().credentials.get(uuid);
    } else if (type != null) {
      StorkCred cred = StorkCred.newFromType(type);
      asAd().unmarshal(cred);
      return cred;
    } else {
      // TODO: What should we do here?
      return null;
    }
  }
}
