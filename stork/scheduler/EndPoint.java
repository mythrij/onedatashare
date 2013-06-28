package stork.scheduler;

import stork.ad.*;
import stork.util.*;
import stork.cred.*;
import stork.module.*;

import java.net.URI;

// A descriptor of an end-point from a job ad. Automatically handles
// lookup of cred token and transfer module.
// TODO: Support for multiple end-points?

public class EndPoint extends Ad {
  private Ad parent;

  public final URI uri;
  public final String proto;
  public final String path;
  public final TransferModule tm;
  public final StorkCred cred;

  private static TransferModuleTable tmt = TransferModuleTable.instance();
  private static CredManager cm = CredManager.instance();

  // Create a new endpoint, optionally with a parent ad to reference
  // missing fields from.
  public EndPoint(String uri) {
    this(new Ad("uri", uri));
  } public EndPoint(Ad ad) {
    super(false, ad);
    //filter("uri", "module", "cred_token");

    uri = StorkUtil.makeURI(get("uri"));
    proto = uri.getScheme();
    path = uri.getPath();
    tm = tmt.byProtocol(proto);
    cred = cm.getCred(get("cred_token"));

    if (tm == null)
      throw new FatalEx("could not find transfer module for: "+proto);
  }

  // Create a URI from either
  public static EndPoint deserialize(String uri) {
    return new EndPoint(uri);
  } public static EndPoint deserialize(Ad ad) {
    return new EndPoint(ad);
  }

  // Create a session for this end-point.
  public StorkSession session() {
    return tm.session(uri, this);
  }

  // Get or set the parent ad.
  public synchronized Ad parent() {
    return parent;
  } public synchronized void parent(Ad p) {
    parent = p;
  }

  // Delegate to the parent ad if the requested key is missing.
  public synchronized Object getObject(Object key) {
    Object o = super.getObject(key);
    return (o == null || parent == null) ? o : parent.getObject(key);
  }
}
