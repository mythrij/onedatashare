package stork.scheduler;

import stork.ad.*;
import stork.cred.*;
import stork.module.*;
import stork.user.*;
import stork.util.*;

import java.net.URI;

// A descriptor of an end-point from a job ad. Automatically handles
// lookup of cred token and transfer module.
// TODO: Support for multiple end-points?
// TODO: Delegate lookups to job ad.

public class EndPoint {
  private URI uri;
  private AdObject cred;
  private String module;
  private transient User user;

  // Create a new endpoint from a URI.
  private EndPoint() {
    // Only used by deserialization.
  } public EndPoint(String s) {
    uri(s);
  } public EndPoint(URI u) {
    uri(u);
  } public EndPoint(Ad ad) {
    this(null, ad);
  } public EndPoint(User user, Ad ad) {
    uri(ad.get("uri"));
    cred(ad.getObject("cred"));
    module(ad.get("module"));
    if (user != null)
      setUser(user);
  }

  // Unmarshal strings as end-points with a URI.
  public static EndPoint unmarshal(String u) {
    return new EndPoint(u);
  }

  // Unmarshal ads end-points with constructor.
  public static EndPoint unmarshal(Ad ad) {
    return new EndPoint(ad);
  }

  // Quick hack so we can perform lookups in the context of a scheduler.
  // Be sure to set this before doing anything else.
  public void setUser(User u) {
    user = u;
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
    return uri = uri.normalize();
  }

  // Get or set the cred token. This AdObject hack lets us accept anonymous
  // credentials until a better solution is found.
  public void cred(String c) {
    cred(AdObject.wrap(c));
  } public void cred(Ad ad) {
    cred(AdObject.wrap(ad));
  } public void cred(AdObject c) {
    cred = c;
  } public StorkCred<?> cred() {
    return (cred == null)    ? null :
           (cred.isString()) ? user.creds.getCred(cred.asString()) :
           (cred.isAd())     ? StorkCred.create(cred.asAd()) : null;
  }

  // Get or set the transfer module.
  public void module(String m) {
    module = m;
  } public TransferModule module() {
    TransferModuleTable xm = TransferModuleTable.instance();
    if (module == null)
      return xm.byProtocol(proto());
    return xm.byHandle(module);
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
}
