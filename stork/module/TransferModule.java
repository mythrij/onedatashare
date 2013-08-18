package stork.module;

import stork.ad.*;
import stork.util.*;
import stork.scheduler.*;
import java.net.URI;

// Abstract base class for a Stork transfer module.

public abstract class TransferModule {
  public String name = null, version = null;
  public String[] protocols = null;
  public String handle = null;
  public String author = null, email = null, website = null;
  public String[] options = null;

  // Subclasses should pass a module info ad to this constructor.
  public TransferModule(Ad ad) {
    ad.unmarshal(this);

    if (name == null || name.isEmpty())
      throw new RuntimeException("module has no name");
    handle = StorkUtil.normalize((handle == null) ? name : handle);
    if (handle.isEmpty())
      throw new RuntimeException("module has an invalid handle: \""+handle+'"');
    if (protocols == null || protocols.length < 1)
      throw new RuntimeException("module does not handle any protocols");
  }

  // Unmarshal transfer modules by name.
  public static TransferModule unmarshal(String s) {
    return byHandle(s);
  }

  // Lookup a transfer module by name.
  public static TransferModule byHandle(String s) {
    return TransferModuleTable.instance().byHandle(s);
  }

  // Lookup a transfer module by protocol.
  public static TransferModule byProtocol(String s) {
    return TransferModuleTable.instance().byProtocol(s);
  }

  // Start a session and perform a listing.
  public Ad list(URI uri) {
    return list(new EndPoint(uri));
  } public Ad list(EndPoint ep) {
    try {
      return session(ep).list(ep.path(), null);
    } catch (Exception e) {
      throw new FatalEx("couldn't list: "+e.getMessage());
    }
  }

  // Create a new session capable of interacting with a URI.
  public StorkSession session(String uri) {
    return session(new EndPoint(uri));
  } public StorkSession session(URI uri) {
    return session(new EndPoint(uri));
  } public abstract StorkSession session(EndPoint e);

  public String toString() {
    return handle;
  }
}
