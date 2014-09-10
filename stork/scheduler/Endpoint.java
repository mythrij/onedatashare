package stork.scheduler;

import java.util.*;
import java.util.concurrent.*;

import stork.*;
import stork.ad.*;
import stork.core.*;
import stork.util.*;
import stork.module.*;
import stork.feather.*;
import static stork.scheduler.JobStatus.*;

public abstract class Endpoint {
  private URI uri;
  private String credential;
  private String module;

  protected abstract Map<String,Credential> credentials();
  protected abstract ModuleTable modules();

  public Resource select() {
    if (uri == null)
      throw new RuntimeException("No URI provided.");
    ModuleTable mt = modules();
    Module m;
    if (module != null)
      m = mt.byHandle(module);
    else
      m = mt.byProtocol(uri.scheme());
    Credential credential = credentials().get(this.credential);
    return m.select(uri, credential);
  }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Endpoint)) return false;
    Endpoint ep = (Endpoint) o;
    if (!uri.equals(ep.uri))
      return false;
    if (credential == null)
      return ep.credential == null;
    return credential.equals(ep.credential);
  }

  public int hashCode() {
    return 1 + 13*uri.hashCode() +
           (credential != null ? 17*credential.hashCode() : 0) +
           (module != null ? 23*module.hashCode() : 0);
  }
}
