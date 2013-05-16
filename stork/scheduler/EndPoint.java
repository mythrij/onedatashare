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
  public final URI uri;
  public final String proto;
  public final String path;
  public final TransferModule tm;
  public final StorkCred cred;

  private static TransferModuleTable tmt = TransferModuleTable.instance();
  private static CredManager cm = CredManager.instance();

  public EndPoint(Ad ad) {
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

  // Create a session for this end-point.
  public StorkSession session() {
    return tm.session(uri, this);
  }
}
