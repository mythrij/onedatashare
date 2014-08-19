package stork.core.handlers;

import stork.core.server.*;
import stork.feather.*;

/** Request common to many commands that operate on endpoints. */
public class EndpointRequest extends Request {
  URI uri;
  String credential;
  String module;

  public Resource resolve() {
    if (uri == null)
      throw new RuntimeException("No URI provided.");
    Credential cred = null;
    if (user != null)
      cred = user.credentials.get(credential);
    if (module != null)
      return server.modules.byHandle(module).select(uri, cred);
    else
      return server.modules.byProtocol(uri.scheme()).select(uri, cred);
  }
}
