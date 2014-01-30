package stork.scheduler;

import stork.ad.*;
import stork.cred.*;
import stork.module.*;
import stork.util.*;

import java.net.URI;

// A description of an endpoint in a client request.

public class Endpoint {
  public URI[] uri;
  public StorkCred cred;
  public String module;

  // Create an endpoint from a set of URI strings or URI objects.
  private Endpoint() {
    // Used for unmarshalling.
  } public Endpoint(String... s) {
    new Ad("uri", s).unmarshal(this);
  } public Endpoint(URI... u) {
    new Ad("uri", u).unmarshal(this);
  }

  // Get the URI, checking for validity.
  public URI[] uri() {
    if (uri == null || uri.length < 1)
      throw new RuntimeException("No URI specified");
    if (uri[0].getScheme() == null)
      throw new RuntimeException("No scheme specified");
    if (!uri[0].isAbsolute() && uri.length > 1)
      throw new RuntimeException("First URI must be absolute");
    if (!uri[0].isAbsolute() && uri.length == 1)
      throw new RuntimeException("URI must be absolute");
    uri[0] = uri[0].normalize();

    URI root = StorkUtil.rootURI(uri[0]);

    // Make sure the rest of the URIs, if any, are relative.
    for (int i = 1; i < uri.length; i++) {
      if (root.relativize(uri[i]).isAbsolute())
        throw new RuntimeException("Additional URIs must be relative");
      uri[i] = uri[i].normalize();
    }

    return uri;
  }

  // Get or set the transfer module.
  public TransferModule module() {
    TransferModuleTable xm = TransferModuleTable.instance();
    if (module == null)
      return xm.byProtocol(proto());
    return xm.byHandle(module);
  }

  public String proto() {
    return uri()[0].getScheme();
  }

  public String path() {
    return uri()[0].getPath();
  }

  // Create a session for this endpoint.
  public StorkSession session() {
    return module().session(this);
  }
}
