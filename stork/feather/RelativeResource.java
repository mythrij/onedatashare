package stork.feather;

/**
 * A {@code Resource} selected from another {@code Resource} by a path.
 */
public final class RelativeResource extends Resource implements Relative {
  private final Resource root;
  private final Path path;

  /**
   * Create a {@code RelativeResource} relative to another {@code
   * RelativeResource} {@code root} by {@code path}. This constructor is
   * preferred if a {@code RelativeResource} has already been resolved to avoid
   * having to reselect the entire relative path from the root.
   *
   * @param root the {@code Resource} this {@code RelativeResource} is relative
   * to.
   * @param path the {@code Path} by which this {@code RelativeResource} is
   * relative to {@code root}.
   */
  public RelativeResource(RelativeResource root, Path path) {
    super(root.select(path));
    this.root = root.root;
    this.path = root.path.append(path);
  }

  /**
   * Create a {@code RelativeResource} relative to {@code root} by {@code
   * path}.
   *
   * @param root the {@code Resource} this {@code RelativeResource} is relative
   * to.
   * @param path the {@code Path} by which this {@code RelativeResource} is
   * relative to {@code root}.
   */
  public RelativeResource(Resource root, Path path) {
    super(root.select(path));
    this.root = root;
    this.path = path;
  }

  public Resource resource() { return this; }

  /**
   * Get the root {@code Resource} this {@code RelativeResource} is relative
   * to.
   *
   * @return The root {@code Resource} of this {@code RelativeResource}.
   */
  public Resource root() { return root; }

  /**
   * Get the path with which this {@code RelativeResource} is relative to its
   * root.
   *
   * @return The path this {@code RelativeResource} is related to its root by.
   */
  public Path path() { return path; }

  public boolean isRoot() {
    return path.isRoot();
  }
}
