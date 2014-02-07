package stork.feather;

// A wrapper for an exception that occurred during a stream operation,
// including relevant contextual information.

public abstract class ResourceError {
  public final Resource resource;
  public final Throwable error;

  public ResourceError(Resource r, Throwable e) {
    resource = r;
    error = e;
  }
}
