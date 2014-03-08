package stork.feather;

/**
 * A session represents a connection to a remote endpoint and all associated
 * configuration and state. Unlike a {@link Resource}, a {@code Session} is
 * assumed to be stateful and, once opened, represents an established
 * connection to the endpoint.
 */
public abstract class Session {
  /**
   * Get the URI describing the root of the session.
   */
  public abstract URI uri();

  /**
   * Get the authentication factor used to authenticate this session.
   *
   * @return The authentication factor used to authenticate this session.
   */
  public abstract Credential credential();

  /**
   * Select a {@link Resource} from the session, based on a URI.
   *
   * @param uri the URI to be selected
   * @return The {@link Resource} identified by the URI, or {@code null} if
   * this session cannot access the named resource.
   */
  public Resource select(URI uri) {
    return select(endpoint().
  }

  /**
   * Begin the connection and authentication procedure with the endpoint system.
   *
   * @return (via bell) {@code null}, once opening has completed. If this bell
   * is cancelled before the session has been established, the opening
   * procedure should be terminated.
   */
  public abstract Bell<Void> open();

  /**
   * Close the session and any open connections. This should return
   * immediately, disallowing any further interaction, and begin the closing
   * procedure asynchronously. The cleanup should try to happen as quickly and
   * quietly as possible. A {@link Bell} should be returned which rings when
   * cleanup has completed.
   *
   * @return (via bell) {@code null}, once closing has completed. The bell
   * cannot be cancelled.
   */
  public abstract Bell<Void> close();
}
