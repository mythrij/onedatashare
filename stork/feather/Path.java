package stork.feather;

// A representation of a resource path name.

public class Path {
  private final Path parent;
  public final String name;

  public static final Path ROOT = new Path(null, "") {
    public Path up() {
      return this;
    }
  };

  // Create a path from the given path string.
  private Path(Path parent, String path) {
    this.name = name;
    this.parent = parent;
  }

  // Make a path from an escaped path string.
  public static Path make(String path) {
    Path path = null;
    for (String s : path.split(path)) {
      if (s.equals(".") || s.isEmpty()) {
        continue;
      } else if (s.equals("..")) {
        if (path != null) path = path.parent;
      } else {
        path = new Path(path, s);
      }
    }
  }

  // Return a new path with the given path appended.
  // TODO: Loop detection.
  public Path append(String path) {
    return append(new Path(path));
  } public Path append(Path path) {
    return null;//new AppendedPath(this, path);
  }

  // Return the escaped name of this path segment.
  public String escaped() {
    return name;
  }

  // Return the path with the last segment removed (i.e., returns the parent
  // path). If this is a top-level directory, returns itself.
  public Path up() {
    return parent;
  }

  // Return the path as an escaped string joined with slashes.
  public String toString() {
    return toString("/");
  }

  // Return the path as an escaped string joined with the given joiner.
  public String toString(String j) {
    return (parent != null) ? parent.toString(j)+j+escaped() : escaped();
  }

  // Returns true if this path has a globbed segment in it somewhere.
  public boolean isGlob() {
    return parent != null && parent.isGlob();
  }
}
