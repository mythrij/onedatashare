package stork.scheduler;

import stork.ad.*;
import stork.util.*;
import stork.cred.*;
import stork.module.*;

import java.net.URI;

// A descriptor of an end-point from a job ad. Automatically handles
// lookup of cred token and transfer module.
// TODO: Support for multiple end-points?

public class EndPoint {
  public URI uri = null;
  public StorkCred cred_token = null;
  public TransferModule module = null;

  private static TransferModuleTable tmt = TransferModuleTable.instance();
  private static CredManager cm = CredManager.instance();

  // Create a new endpoint from a URI.
  public EndPoint() {
    // Does nothing.
  } public EndPoint(String s) {
    this(StorkUtil.makeURI(s));
  } public EndPoint(URI u) {
    if (u == null)
      throw new FatalEx("missing uri field from endpoint");
    uri = u;
  }

  public static EndPoint unmarshal(String s) {
    return new EndPoint(s);
  }

  public String proto() {
    return uri.getScheme();
  }

  public String path() {
    return uri.getPath();
  }

  // Create a session for this endpoint.
  public StorkSession session() {
    if (module == null)
      module = tmt.byProtocol(proto());
    return module.session(this);
  }
}
