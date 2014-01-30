package stork.module;

import stork.ad.*;
import stork.util.*;
import stork.scheduler.*;

import java.util.*;
import java.net.URI;

// Abstract base class for a Stork transfer module.

public abstract class TransferModule {
  public final String name, handle;
  public final String[] protocols;
  protected String description = "(no description)";
  protected String version = "N/V";
  protected String author, email, website;
  protected String[] options;

  public TransferModule(Ad ad) {
    this(ad.get("name"), ad.getAll(String[].class, "protocols"));
  } public TransferModule(String name, String... protocols) {
    // Check the name.
    if (name == null || name.isEmpty())
      throw new RuntimeException("module has no name");
    this.name = name;

    // Generate a handle.
    handle = StorkUtil.normalize(name);
    if (handle.isEmpty())
      throw new RuntimeException("module has an invalid handle: \""+handle+'"');

    // Check and normalize the protocols.
    if (protocols == null || protocols.length < 1)
      throw new RuntimeException("module does not handle any protocols");
    this.protocols = normalizedSet(protocols);
  }

  // Return a normalized string set. Used for protocol and option sets.
  private static String[] normalizedSet(String... s) {
    for (int i = 0; i < s.length; i++)
      s[i] = StorkUtil.normalize(s[i]);
    return new HashSet<String>(Arrays.asList(s)).toArray(new String[0]);
  }

  // Convenience method for setting options.
  protected void options(Class clazz) {
    options = Ad.fieldsOf(clazz);
  } protected void options(String... opts) {
    options = normalizedSet(opts);
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

  // Create a new session capable of interacting with a URI.
  public Session session(String... uri) {
    return session(new EndPoint(uri));
  } public Session session(URI... uri) {
    return session(new EndPoint(uri));
  } public abstract Session session(EndPoint e);

  public String toString() {
    return handle;
  }
}
