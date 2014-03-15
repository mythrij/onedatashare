package stork.module;

import stork.ad.*;
import stork.feather.*;
import stork.util.*;
import stork.scheduler.*;

import java.util.*;

// Abstract base class for a Stork transfer module.

public abstract class Module {
  public final String name, handle;
  public final String[] protocols;
  protected String description = "(no description)";
  protected String version = "N/V";
  protected String author, email, website;
  protected String[] options;

  public Module(Ad ad) {
    this(ad.get("name"), ad.getAll(String[].class, "protocols"));
  } public Module(String name, String... protocols) {
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
  public static Module unmarshal(String s) {
    return byHandle(s);
  }

  // Lookup a transfer module by name.
  public static Module byHandle(String s) {
    return ModuleTable.instance().byHandle(s);
  }

  // Lookup a transfer module by protocol.
  public static Module byProtocol(String s) {
    return ModuleTable.instance().byProtocol(s);
  }

  /**
   * Return a handle on the resource identified by a URI.
   *
   * @param uri a string representation of the URI to select.
   * @return A handle on the resource identified by a URI.
   * @throws IllegalArgumentException if the given URI does not properly
   * identify a resource or specifies a scheme not supported by this module.
   */
  public final Resource select(String uri) {
    return select(uri, null);
  }

  /**
   * Return a handle on the resource identified by a URI using the given
   * authentication factor.
   *
   * @param uri a string representation of the URI to select.
   * @param credential an authentication factor, or {@code null}.
   * @return A handle on the resource identified by a URI.
   * @throws IllegalArgumentException if the given URI does not properly
   * identify a resource or specifies a scheme not supported by this module.
   */
  public final Resource select(String uri, Credential credential) {
    return select(uri, credential);
  }

  /**
   * Return a handle on the resource identified by a URI.
   *
   * @param uri the URI to select.
   * @return A handle on the resource identified by a URI.
   * @throws IllegalArgumentException if the given URI does not properly
   * identify a resource or specifies a scheme not supported by this module.
   */
  public final Resource select(URI uri) {
    return select(uri, null);
  }

  /**
   * Return a handle on the resource identified by a URI using the given
   * authentication factor. Subclasses should implement this to handle the
   * module-specific details of interpretting the URI and instantiating a
   * {@link Resource} (and its underlying {@link Session}) to access the
   * resource indentified by the URI.
   *
   * @param uri the URI to select.
   * @param credential an authentication factor, or {@code null}.
   * @return A handle on the resource identified by a URI.
   * @throws IllegalArgumentException if the given URI does not properly
   * identify a resource or specifies a scheme not supported by this module.
   * @see Resource
   * @see Session
   */
  public abstract Resource select(URI uri, Credential credential);

  public String toString() {
    return handle;
  }
}
