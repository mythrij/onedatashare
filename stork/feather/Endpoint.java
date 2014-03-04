package stork.scheduler;

import stork.ad.*;
import stork.cred.*;
import stork.feather.*;
import stork.util.*;

/**
 * A description of an endpoint resource along with the authentication factors
 * necessary to establish a session with the described endpoint.
 */

public class Endpoint {
  public URI uri;
  public Credential credential;

  /**
   * Create an endpoint with no URI or credential for the purposes of
   * subclassing or unmarshalling.
   */
  protected Endpoint() {
    uri = null;
    credential = null;
  }

  /**
   * Create an endpoint based on the given URI.
   *
   * @param uri the string form of the URI identifying this endpoint
   */
  public Endpoint(String uri) {
    this(uri, null);
  }

  /**
   * Create an endpoint based on the given URI.
   *
   * @param uri the URI identifying this endpoint
   */
  public Endpoint(URI uri) {
    this(uri, null);
  }

  /**
   * Create an endpoint based on the given URI and credential.
   *
   * @param uri the string form of the URI identifying this endpoint
   * @param credential the credential associated with this endpoint
   */
  public Endpoint(String uri, Credential credential) {
    this(URI.create(uri), credential);
  }

  /**
   * Create an endpoint based on the given URI and credential.
   *
   * @param uri the URI identifying this endpoint
   * @param credential the credential associated with this endpoint
   * @throws NullPointerException if {@code uri} is {@code null}
   */
  public Endpoint(URI uri, Credential credential) {
    if (uri == null)
      throw new NullPointerException("uri may not be null");
    this.uri = uri;
    this.credential = credential;
  }

  /**
   * Create a new session capable of interacting with this endpoint, or {@code
   * null} if no default session can be created.
   *
   * @return A session capable of interacting with this endpoint, or {@code
   * null} if no default session can be created.
   * @see Session
   */
  public Session session() {
    return null;
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
           (credential != null ? 17*credential.hashCode() : 0);
  }
}
