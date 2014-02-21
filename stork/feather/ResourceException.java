package stork.feather;

/**
 * A wrapper for an exception that occurred during an operation on a resource,
 * including relevant contextual information.
 */
public class ResourceException extends RuntimeException {
  /** The {@link Resource} the error came from. */
  private Resource resource;

  /**
   * Create a new {@code ResourceException} wrapping the given {@link Resource} and
   * {@link Throwable}.
   *
   * @param resource The {@link Resource} the error associated with the
   * exception.
   * @param error The actual error that occurred.
   */
  public ResourceException(Resource resource, Throwable error) {
    super(error);
    resource = r;
  }

  /**
   * Get the {@link Resource} associated with the wrapped exception.
   *
   * @return The {@link Resource} associated with the wrapped exception.
   */
  public Resource resource() {
    return resource;
  }
}
