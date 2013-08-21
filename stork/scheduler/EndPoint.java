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
  public StorkCred<?> cred_token = null;
  public transient TransferModule module = null;

  private static TransferModuleTable tmt = TransferModuleTable.instance();
  private static CredManager cm = CredManager.instance();

  // Create a new endpoint from a URI.
  public EndPoint() {
    // Does nothing.
  } public EndPoint(String s) {
    this(StorkUtil.makeURI(s));
  } public EndPoint(URI u) {
    if (u == null)
      throw new FatalEx("missing URI field from endpoint");
    uri = u;
  }

  public static EndPoint unmarshal(String s) {
    return new EndPoint(s);
  }

  public String proto() {
    if (uri == null)
      throw new RuntimeException("endpoint has no URI");
    if (uri.getScheme() == null)
      throw new RuntimeException("scheme omitted from URI");
    return uri.getScheme();
  }

  public String path() {
    if (uri == null)
      throw new RuntimeException("endpoint has no URI");
    return uri.getPath();
  }

  // Create a session for this endpoint.
  public StorkSession session() {
    if (module == null)
      module = tmt.byProtocol(proto());
    if (module == null)
      throw new RuntimeException("no module for "+proto()+" registered");
    return module.session(this);
  }

  // Create a session paired with another endpoint session.
  public StorkSession pairWith(EndPoint e) {
    return session().pair(e.session());
  }
}
