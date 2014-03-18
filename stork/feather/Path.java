package stork.feather;

import java.util.*;
import java.io.*;

import stork.feather.util.*;

/**
 * A memory-efficient representation of a path name.
 */
public abstract class Path {
  private final String name;
  private transient int hash = 0;

  private static final Intern<Path> INTERN = new Intern<Path>();

  public Path(String name) {
    this.name = name;
  }

  // The root path, which should be the only path with a blank name.
  public static final Path ROOT = new Path("") {
    public Path up()         { return this; }
    public int length()      { return 0; }
    public String toString(boolean escaped) {
      return escaped ? "" : "/";
    }
  };

  /**
   * Return the parent of this path segment.
   * @return The parent of this path segment.
   */
  public abstract Path up();

  /**
   * Paths should be created using this static method, which will take care of
   * interning segments and unescaping segment names.
   */
  public static Path create(String path) {
    return create(ROOT, path);
  } private static Path create(Path par, String path) {
    String[] ps = popSegment(path);
    return (ps == null) ? par : create(par, ps[0]).appendSegment(ps[1]);
  }

  // Helper method for trimming trailing slashes and splitting the last
  // segment. This method returns null if the path represents the root
  // directory. Otherwise, it returns an array containing the remaining path
  // string and the last segment.
  private static String[] popSegment(String p) {
    int s, e = p.length()-1;
    while (e > 0 && p.charAt(e) == '/') e--;  // Trim slashes.
    if (e <= 0) return null;  // All slashes (or empty).
    s = p.lastIndexOf('/', e);
    if (s < 0) s = 0;
    return new String[] { p.substring(0, s), p.substring(s+1, e+1) };
  }

  /**
   * Return a new path with the given path appended. This is implemented using
   * a wrapper around the passed path that delegates operations to the wrapped
   * path.
   *
   * @param path an escaped string representation of a path
   */
  public Path append(String path) {
    return create(this, path);
  }

  /**
   * Return a new path with the given path appended. This is implemented using
   * a wrapper around the passed path that delegates operations to the wrapped
   * path.
   *
   * @param path a path to append
   * @return a path with the given path appended
   */
  public Path append(final Path path) {
    if (path.isRoot() && path.name().isEmpty())
      return this;
    return new Path(path.name) {
      public Path up() {
        return path.up().isRoot() ? Path.this : Path.this.append(path.up());
      }
      public String toString(boolean escaped) {
        return Path.this.toString(escaped)+path.toString(escaped);
      }
      public int length() {
        return Path.this.length()+path.length();
      }
    };
  }

  /**
   * Create a path segment with the given name whose parent is this path.
   *
   * @param name the unescaped name of a segment
   */
  public Path appendSegment(final String name) {
    if (name.equals("."))
      return this;
    if (name.equals(".."))
      return up();
    return new Path(name) {
      public Path up() { return Path.this; }
    };
  }

  /**
   * Return the unescaped name of this path segment.
   * @return The unescaped name of this path segment.
   */
  public final String name() {
    return name;
  }

  /**
   * Return the name of this path segment, either escaped or unescaped.
   *
   * @param escaped whether or not the escape the segment name
   */
  public final String name(boolean escaped) {
    return escaped ? URI.escape(name) : name;
  }

  /**
   * Check if this path segment is a rooit. Roots are any paths whose parents
   * are themselves.
   */
  public final boolean isRoot() {
    return up() == this;
  }

  /**
   * Return a string representation of the path.
   *
   * @param escaped whether or not the escape the segment names
   */
  public String toString(boolean escaped) {
    String j = "/";
    return up().isRoot() ? up().name(escaped)+j+name(escaped) :
           isRoot()      ? name(escaped) : up()+j+name(escaped);
  }

  /**
   * Return an escaped string representation of the path.
   */
  public String toString() {
    return toString(true);
  }

  /**
   * Return an unescaped string representation of the path to be read by a
   * human. Note that this representation cannot be always be unambiguously
   * converted back into the original path, and should only be used for
   * presenting the path to a human for information purposes.
   *
   * @return An unescaped string representation of this path.
   */
  public final String readable() {
    return toString(false);
  }

  /**
   * Returns whether this is a globbed path. That is, whether or not this path
   * has a glob segment in it somewhere.
   *
   * @return {@code true} if this path has a glob segment in it somewhere;
   * {@code false} otherwise
   */
  public boolean isGlob() {
    //return up().isGlob();
    return false;
  }

  /**
   * Return the number of segments in the path.
   * @return The number of segments in the path.
   */
  public int length() {
    return up().length()+1;
  }

  /**
   * The hash code of a path should be equal to the hash code of the string
   * representation of the path. Of course we should do this without actually
   * stringifying the entire path, which we can with a little bit of math. The
   * hash code of a string is defined in the Java documentation.
   */
  public int hashCode() {
    return (hash != 0) ? hash : (hash = toString().hashCode());

    /*
    int h = hash;

    if (h != 0)
      return h;
    if (isRoot())
      return hash = toString().hashCode();
    return hash = up().hashCode()*31 + '/'*31 + 

    return h;
    */
  }

  /**
   * Check if two paths are equal. Two paths are equal if they are
   * component-wise equal.
   */
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } if (o instanceof Path) {
      Path p = (Path) o;
      return componentEquals(p) && up().equals(p.up());
    } return false;
  }

  /**
   * Check if two path end components are equal in their escaped
   * representations.
   */
  public boolean componentEquals(Path p) {
    return toString().equals(p.toString());
  }

  /**
   * Check if the paths match through globbing.
   */
  public boolean matches(Path p) {
    return equals((Object)p);
  }

  public static void main(String args[]) throws Exception {
    Path p1 = Path.create("/home/globus");
    while (true) {
      p1 = p1.append(p1);
      System.out.println(p1.length());
    }

    /*
    BufferedReader r = new BufferedReader(new InputStreamReader(System.in));

    String sp = r.readLine();
    Path p = Path.create(sp);
    while (true) {
      sp = r.readLine();
      p = p.append(Path.create(sp));
      System.out.println(p);
    }
    */
  }
}
