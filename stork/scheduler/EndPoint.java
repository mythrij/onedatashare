package stork.scheduler;

import stork.ad.*;
import stork.util.*;
import stork.cred.*;
import stork.module.*;

import java.net.URI;

// A descriptor of an end-point from a job ad. Automatically handles
// lookup of cred token and transfer module.
// TODO: Support for multiple end-points?
// TODO: Delegate lookups to job ad.

public class EndPoint {
  private URI uri;
  private String cred;
  private String module;
  private transient StorkScheduler sched = null;

  // Create a new endpoint from a URI.
  private EndPoint() {
    // Only used by deserialization.
  } public EndPoint(String s) {
    uri(s);
  } public EndPoint(URI u) {
    uri(u);
  } public EndPoint(Ad ad) {
    uri(ad.get("uri"));
    cred(ad.get("cred"));
    module(ad.get("module"));
  }

  // Unmarshal strings as end-points with a URI.
  public static EndPoint unmarshal(String u) {
    return new EndPoint(u);
  }

  // Quick hack so we can perform lookups in the context of a scheduler.
  // Be sure to set this before doing anything else.
  public void scheduler(StorkScheduler s) {
    sched = s;
    validate();
  }

  // Validate that this thing was created properly.
  public void validate() {
    uri();
    cred();
    module();
  }

  // Get or set the URI.
  public void uri(String s) {
    uri(StorkUtil.makeURI(s));
  } public void uri(URI u) {
    uri = u;
  } public URI uri() {
    if (uri == null)
      throw new RuntimeException("missing URI");
    if (uri.getScheme() == null)
      throw new RuntimeException("no scheme specified");
    return uri;
  }

  // Get or set the cred token.
  public void cred(String c) {
    cred = c;
  } public StorkCred<?> cred() {
    return (cred != null) ? sched.creds.get(cred) : null;
  }

  // Get or set the transfer module.
  public void module(String m) {
    module = m;
  } public TransferModule module() {
    if (module == null)
      return sched.xfer_modules.byProtocol(proto());
    return sched.xfer_modules.byHandle(module);
  }

  public String proto() {
    return uri().getScheme();
  }

  public String path() {
    return uri().getPath();
  }

  // Create a session for this endpoint.
  public StorkSession session() {
    return module().session(this);
  }

  // Create a session paired with another endpoint session.
  public StorkSession pairWith(EndPoint e) {
    return session().pair(e.session());
  }
}
