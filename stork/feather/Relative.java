package stork.feather;

/**
 * An interface for objects wrapped with context regarding its origin in an
 * ongoing transfer. This wraps information such as the {@code Resource} of
 * origin, the root {@code Resource} the originating {@code Resource} is
 * relative to, and the selection path of the 
 */
public interface Relative {
  /**
   * Get the concrete {@code Resource} this object is relative to. This will be
   * equalvalent to {@link #root()} if and only if {@link #isRoot()} returns
   * {@code true}.
   *
   * @return the {@code Resource} this object is relative to.
   */
  Resource resource();

  /**
   * Get the root {@code Resource} context of the wrapped object. This will be
   * equalvalent to {@link #resource()} if and only if {@link #isRoot()}
   * returns {@code true}.
   */
  Resource root();

  /**
   * Get the path with which the origin is relative to the transfer root.
   *
   * @return The path of the originating resource relative to the root.
   */
  Path path();

  /**
   * Check if this wrapped object originated from the root
   *
   * @return {@code true} if this {@code RelativeResource} is the root
   * resource; {@code false} otherwise.
   */
  boolean isRoot();
}
