package stork.feather;

/**
 * A {@code Slice} which includes a reference to the {@code RelativeResource} it originated from.
 */
public class RelativeSlice extends Slice {
  private final RelativeResource resource;

  /**
   * Create a {@code RelativeSlice} wrapping {@code slice} and with the
   * associated {@code RelativeResource} {@code resource}.
   *
   * @param resource the {@code RelativeResource} of origin.
   * @param slice the {@code Slice} to wrap.
   */
  public RelativeSlice(RelativeResource resource, Slice slice) {
    super(slice);
    this.resource = resource;
  }

  /**
   * Get the {@code RelativeResource} associated with this {@code Slice}.
   */
  public RelativeResource resource() {
    return resource;
  }

  /**
   * Get the root {@code Resource} the associated {@code RelativeResource} is
   * relative to.
   *
   * @return The root {@code Resource} of the associated {@code
   * RelativeResource}.
   */
  public Resource root() { return resource.root(); }

  /**
   * Get the path with which the associated {@code RelativeResource} is
   * relative to its root.
   *
   * @return The path the associated {@code RelativeResource} is related to
   * its root by.
   */
  public Path path() { return path; }
}
