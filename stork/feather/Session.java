package stork.feather;

/**
 * A session represents a connection to a remote endpoint and all associated
 * configuration and state, as well as a root resource from which subresources
 * may be selected. Unlike a {@link Resource}, a {@code Session} is assumed to
 * be stateful and its instantiation represents the establishment of a
 * connection to an endpoint. Generally implementations should implement this
 * interface by extending their own {@link Resource} implementation and then
 * implementing this interface's methods.
 */
public interface Session extends Resource {
  /**
   * Close the session and free any resources. This should return immediately,
   * disallowing any further interaction, and begin the closing procedure
   * asynchronously. The cleanup should try to happen as quickly and quietly as
   * possible. A {@link Bell} should be returned which rings when cleanup has
   * completed.
   *
   * @return (via bell) {@code null}, once closing has completed.
   */
  Bell<Void> close();

  /**
   * Create an identical session with the same settings.
   */
  //Session duplicate();
}
