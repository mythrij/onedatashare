package stork.feather;

import java.util.*;
import java.util.regex.*;
import java.io.*;

import stork.feather.util.*;

/**
 * A representation of an absolute {@code Path} adhering to RFC 3986's
 * definition of URI path components extended to support glob pattern matching.
 * <p/>
 * This class is designed to be memory-efficient for storing very large path
 * trees. The instantiation of new {@code Path}s is controlled internally, and
 * {@link #create(String)} must be used to parse strings into {@code Path}
 * objects.
 * <p/>
 * All paths are absolute paths and will never contain components with the
 * names {@code ".."} or {@code "."}.
 * <p/>
 * {@code Path} objects are immutable and are safe to use as keys in a map or
 * entries in a set.
 */
public abstract class Path {
  private transient int hash = 0;

  static final Intern<Path> INTERN = new Intern<Path>();
  static final String[] EMPTY_SEGMENT_ARRAY = new String[0];

  // Paths can only be constructed in this package. This isn't class private
  // because URI extends Path and is in a separate source file.
  Path() { }

  /**
   * The top-level parent of all absolute {@code Path}s. It cannot be traversed
   * above.
   */
  public static final Path ROOT = new RootPath();

  /**
   * A top-level "." segment, used to represent an empty relative {@code Path}.
   */
  public static final Path DOT = DotPath.DOT;

  /**
   * A top-level ".." segment.
   */
  public static final Path DOTDOT = DotPath.DOTDOT;

  /**
   * Return the parent of this {@code Path}.
   *
   * @return The parent of this {@code Path}.
   */
  public abstract Path up();

  /**
   * Return the name of this path segment, either escaped or unescaped.
   *
   * @param encoded whether or not to URI encode the segment name.
   */
  public abstract String name(boolean encoded);

  /**
   * Return a glob {@code Path} that matches both this {@code Path} and {@code
   * path}.
   *
   * @return A glob {@code Path} matching both this and the given path.
   */
  //public abstract Path glob(Path path);


  /**
   * Return a new {@code Path} with this {@code Path} appended to the given
   * {@code Path}.
   *
   * @param path a {@code Path} to append this {@code Path} to.
   * @return A {@code Path} with this {@code Path} appended.
   */
  public abstract Path appendTo(Path path);

  /**
   * Check if the last segments of this {@code Path} and the given {@code Path}
   * are equal. This will only check if the end segments match. Use {@link
   * #equals(Object)} to test for matches over all segments.
   *
   * @param path the {@code Path} to test last segments against.
   * @return {@code true} if this {@code Path}'s last segment equals the given
   * {@code Path}'s last segment; {@code false} otherwise.
   */
  abstract boolean segmentEquals(Path path);

  /**
   * Check if the last segment of {@code Path} matches the last segment of the
   * given {@code Path}. This will only check if the end segments match. Use
   * {@link #matches(Path)} to test for matches over all segments.
   *
   * @param path the {@code Path} to match last segments against.
   * @return {@code true} if this {@code Path}'s last segment matches the given
   * {@code Path}'s last segment; {@code false} otherwise.
   */
  public abstract boolean segmentMatches(Path path);

  /**
   * Return the {@code n}th parent of this {@code Path}. If {@code n == 0},
   * this {@code Path} is returned. If {@code n > 0}, this is equivalent to
   * calling {@link #up()} {@code n} times.
   *
   * @param n the number of segments to remove.
   * @return The parent of this path segment.
   * @throws IllegalArgumentException if {@code n < 0}.
   */
  public Path up(int n) {
    if (n < 0)
      throw new IllegalArgumentException();
    if (n == 0)
      return this;
    return up().up(n-1);
  }

  /**
   * Return the top-level parent of this {@code Path} segment.
   *
   * @return The parent of this path segment.
   */
  public Path root() { return isRoot() ? this : up().root(); }

  /**
   * {@code Path}s should be created using this static method, which will take care of
   * interning segments and unescaping segment names.
   *
   * @param path an escaped string representation of a path. 
   * @return The {@code Path} represented by {@code path}.
   */
  public static Path create(String path) {
    return create(ROOT, path);
  }

  private static Path create(Path par, String path) {
    String[] ps = popSegment(path);
    return (ps == null) ? par : create(par, ps[0]).append(ps[1]);
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
    if (s < 0)
      return new String[] { "", p.substring(0, e+1) };
    return new String[] { p.substring(0, s), p.substring(s+1, e+1) };
  }

  /**
   * Parse {@code path} as an escaped path string and append it to this {@code
   * Path}.
   *
   * @param path an escaped {@code String} representation of a {@code Path}.
   * @return 
   */
  public final Path append(String path) {
    return create(this, path);
  }

  /**
   * Return a new {@code Path} with the given {@code Path} appended. This is
   * equivalent to {@code path.appendTo(this)}.
   *
   * @param path a path to append.
   * @return A {@code Path} with the given {@code Path} appended.
   */
  public final Path append(Path path) {
    return path.appendTo(this);
  }

  /**
   * Append a segment to this {@code Path} whose unescaped name is {@code
   * name}. No special interpretation of {@code name} will be done, with the
   * exception that the {@code String}s {@code "."} and {@code ".."} will cause
   * {@code this} and {@link #up()}, respectively, to be returned.
   *
   * @param name the unescaped name of the segment to append.
   * @return A {@code Path} with a segment whose unescaped name is {@code name}
   * appended.
   */
  public Path appendLiteral(String name) {
    if (name.equals("."))
      return this;
    if (name.equals(".."))
      return up();
    return append(URI.encode(name));
  }

  /**
   * Return the unescaped name of this path segment.
   *
   * @return The unescaped name of this path segment.
   */
  public final String name() {
    return name(false);
  }

  /**
   * Check if this {@code Path} is the prefix of another {@code Path}.
   *
   * @param path the path to check if this path is a prefix of.
   */
  public boolean prefixes(Path path) {
    if (path == this)
      return true;
    if (this.isRoot())
      return true;
    if (path.isRoot())
      return false;
    if (this.name().equals(path.name()))
      return this.up().prefixes(path.up());
    return this.prefixes(path.up());
  }

  /**
   * Check if this {@code Path} is a root {@code Path}.
   *
   * @return {@code true} if this is the root {@code Path}; {@code false}
   * otherwise.
   */
  public boolean isRoot() { return false; }

  /**
   * Check if this {@code Path} is absolute. An absolute {@code Path} is one
   * whose root is the root {@code Path}, i.e. {@code Path.ROOT}.
   *
   * @return {@code true} if this is an absolute {@code Path}; {@code false}
   * otherwise.
   */
  public boolean isAbsolute() {
    return up().isAbsolute();
  }

  /**
   * Check if this {@code Path} is relative.
   *
   * @return {@code true} if this is a relative {@code Path}; {@code false}
   * otherwise.
   */
  public final boolean isRelative() {
    return !isAbsolute();
  }

  /**
   * Return an absolute {@code Path}. This is equivalent to {@code
   * appendTo(Path.ROOT)}.
   *
   * @return This {@code Path} absolutized against the root {@code Path}.
   */
  public Path absolutize() { return appendTo(Path.ROOT); }

  /**
   * Absolutize this {@code Path} against {@code root}. The returned {@code
   * Path} will not traverse beyond {@code root}.
   *
   * @return This {@code Path} absolutized against {@code root}.
   */
  public Path absolutize(Path root) { return root.append(absolutize()); }

  /**
   * Explode this path into its unescaped component names.
   *
   * @return An array of the names of this path's components.
   */
  public final String[] explode() {
    if (isRoot())
      return EMPTY_SEGMENT_ARRAY;
    String[] list = new String[length()];
    Path p = this;
    for (int i = list.length-1; i >= 0; i--) {
      list[i] = p.name();
      p = p.up();
    }
    return list;
  }

  /**
   * Implode an array of literal component names into a {@code Path}.
   *
   * @param names component names to implode into a {@code Path}.
   * @return {@code names} merged into a {@code Path}.
   */
  public static Path implode(String... names) {
    Path p = ROOT;
    for (String n : names)
      p = p.appendLiteral(n);
    return p;
  }

  /**
   * Check whether or not this is a glob {@code Path}. That is, whether or not
   * this {@code Path} has a glob segment in it somewhere.
   *
   * @return {@code true} if this {@code Path} has a glob segment in it
   * somewhere; {@code false} otherwise.
   */
  public boolean isGlob() {
    return up().isGlob();
  }

  /**
   * Return the first glob segment of this {@code Path}.  If the {@code Path}
   * is a trunk {@code Path}, this {@code Path} is returned.
   *
   * @return The non-glob prefix {@code Path} of this {@code Path}.
   */
  public Path firstGlob() {
    if (isRoot())
      return this;
    Path glob = up().firstGlob();
    return (glob.isGlob()) ? glob : this;
  }

  /**
   * Return the trunk of this {@code Path}. That is, return the longest prefix
   * of this {@code Path} that is not a glob {@code Path}. If the {@code Path}
   * is already a trunk {@code Path}, this {@code Path} is returned.
   *
   * @return The trunk of this {@code Path}.
   */
  public Path trunk() {
    if (isRoot())
      return this;
    Path trunk = up().trunk();
    return (isGlob()) ? trunk : this;
  }

  /**
   * Check if this {@code Path} covers the given {@code Path}.
   *
   * @return {@code true} if this {@code Path} covers {@code path}; {@code
   * false} otherwise.
   */
  public boolean covers(Path path) {
    // TODO
    return false;
  }

  /**
   * Return the number of segments in this {@code Path}. This will always be a
   * non-negative number.
   *
   * @return The number of segments in the {@code Path}.
   */
  public int length() { return up().length()+1; }

  /**
   * Truncate the {@code Path} to the given {@code length}.
   */
  public Path truncate(int length) {
    if (length < 0)
      throw new IllegalArgumentException();
    if (length == 0)
      return root();
    int pos = length();
    if (length >= pos)
      return this;
    return up().truncate(length, pos-1);
  } private Path truncate(int length, int pos) {
    return (length >= pos) ? this : truncate(length, pos-1);
  }

  /**
   * Convert an escaped glob {@code String} into a regular expression. The
   * {@code String} returned by this method can be used to construct a {@code
   * Pattern} matching the {@code CharSequence}s the glob pattern matches. In
   * other words, this transforms a glob expression into a regular expression.
   *
   * @param glob an escaped glob expression to create a regular expression
   * from.
   * @return A regular expression {@code String} based on {@code glob}.
   */
  public static String globToRegex(String glob) {
    StringBuffer sb = new StringBuffer();
    String[] sa = glob.split("*", -1);
    
    if (sa.length != 0) {
      sb.append(Pattern.quote(sa[0]));
      for (int i = 1; i < sa.length; i++)
        sb.append(".*"+Pattern.quote(sa[i]));
    }

    return sb.toString();
  }

  /**
   * Check if this {@code Path} specifies a pattern which matches {@code path}.
   * Note that this relation is not symmetrical. This {@code Path} 
   * Specifically, this {@code Path} matches another {@code Path} if 
   *
   * @param path the {@code Path} to match against.
   * @return {@code true} if this {@code Path} matches the given {@code Path};
   * {@code false} otherwise.
   */
  public boolean matches(Path path) {
    if (path == this)
      return true;
    if (!segmentMatches(path))
      return false;
    return up().matches(path.up());
  }

  /**
   * Check if two {@code Path}s are equal. That is, check that two {@code
   * Path}s are component-wise equal.
   *
   * @param object the object to test equality against.
   * @return {@code true} if {@code object} is a {@code Path} and equals this
   * {@code Path}; {@code false} otherwise.
   */
  public boolean equals(Object object) {
    if (object == this)
      return true;
    if (!(object instanceof Path))
      return false;
    Path path = (Path) object;
    if (path.hash != 0 && hash != 0 && path.hash != hash)
      return false;
    return segmentEquals(path) && up().equals(path.up());
  }

  /**
   * The hash code of a path should be equal to the hash code of the string
   * representation of the path.
   */
  public int hashCode() {
    // TODO: Of course we should do this without actually stringifying the
    // entire path, which we can with a little bit of math. The hash code of a
    // string is defined in the Java documentation.
    return (hash != 0) ? hash : (hash = toString().hashCode());
  }

  /**
   * Return an escaped string representation of this {@code Path}.
   *
   * @return An escaped string representation of this {@code Path}.
   */
  public String toString() {
    return (isRoot()) ? name() :
           (up() == ROOT) ? "/"+name(true) : up()+"/"+name(true);
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

class RootPath extends Path {
  public Path up() { return this; }
  public String name(boolean e) { return "/"; }
  public int length() { return 0; }
  public boolean isRoot() { return true; }
  public boolean isAbsolute() { return true; }
  public Path appendTo(Path path) { return path; }
  public boolean segmentMatches(Path p) { return p == this; }
  public boolean segmentEquals(Path p) { return p == this; }
}

class DotPath extends RootPath {
  private final int depth;

  private DotPath(int depth) { this.depth = depth; }

  public static DotPath DOT = new DotPath(0) {
    public String name(boolean e) { return "."; }
    public String toString() { return "."; }
    public int length() { return 1; }
  };
  public static DotPath DOTDOT = new DotPath(1);

  static DotPath create(int depth) {
    switch (depth) {
      case 0 : return DOT;
      case 1 : return DOTDOT;
      default: return new DotPath(depth);
    }
  }

  public Path up() { return create(depth+1); }
  public int length() { return depth; }
  public String name(boolean e) { return ".."; }
  public boolean isAbsolute() { return false; }
  public Path appendTo(Path path) { return path.up(depth); }
  public boolean segmentMatches(Path p) { return equals(p); }

  public boolean segmentEquals(Path p) {
    return (p instanceof DotPath) && ((DotPath)p).depth == depth;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder("..");
    for (int i = 1; i < depth; i++)
      sb.append("/..");
    return sb.toString();
  }
}

// A path segment that matches an exact string.
class LiteralPath extends Path {
  protected final String name;
  protected final Path up;

  // Name must be unescaped.
  public LiteralPath(Path up, String name) {
    this.up = up;
    this.name = name;
  }

  public Path up() { return up; }

  public Path appendTo(Path path) {
    path = new LiteralPath(up, name);
    return Path.INTERN.intern(path);
  }

  public String name(boolean encode) {
    return encode ? URI.encode(name) : name;
  }

  public boolean segmentMatches(Path path) {
    if (path instanceof LiteralPath)
      return name.equals(path.name(false));
    else if (path instanceof GlobPath)
      return path.matches(this);
    return false;
  }

  public boolean segmentEquals(Path path) {
    if (path instanceof LiteralPath)
      return name.equals(path.name(false));
    return name(true).equals(path.name(true));
  }
}

// A path whose last segment is a glob expression.
class GlobPath extends Path {
  protected final Path up;
  protected final String name;
  protected Pattern pattern;

  // Name must be escaped.
  public GlobPath(Path up, String name) {
    this.up = up;
    this.name = name;
  }

  public GlobPath(Path up, String name, Pattern pattern) {
    this.up = up;
    this.name = name;
    this.pattern = pattern;
  }

  public Path up() { return up; }

  public String name(boolean encode) {
    return encode ? name: URI.decode(name);
  }

  private synchronized Pattern pattern() {
    if (pattern != null)
      return pattern;
    return pattern = Pattern.compile(Path.globToRegex(name));
  }

  public boolean isGlob() { return true; }

  public Path appendTo(Path path) {
    path = new GlobPath(up, name);
    return Path.INTERN.intern(path);
  }

  public boolean segmentMatches(Path path) {
    return pattern().matcher(path.name(true)).matches();
  }

  public boolean segmentEquals(Path path) {
    if (path instanceof GlobPath)
      return name.equals(path.name(false));
    return name(true).equals(path.name(true));
  }
}

// A path segment that can match subpaths.
/*
class PathGroup extends SegmentPath {
  private final Path[] paths;

  PathGroup(Path up, Path... paths) {
    super(up);
    this.paths = paths;
  }

  protected Path appendCloneTo(Path path) {
    return new SegmentPath(path, paths);
  }

  public boolean matches(Path path) {
    for (Path p : paths)
      if (append(p).matches(path)) return true;
    return false;
  }

  public boolean equals(Path path) {
    if (path instanceof PathGroup)
      
  }
}

// A path segment that matches any path suffix.
class DoubleStarPath extends SegmentPath {
  public DoubleStarPath(Path up) { super(up); }
  public String name(boolean escaped) { return "**"; }

  protected Path appendCloneTo(Path path) {
    return new DoubleStarPath(path);
  }

  public boolean matches(Path path) {
    return matches(path, depth(), path.depth());
  } private boolean matches(Path path, int d) {
    return up.matches(path) || (d > 0 && matches(path.up(), d-1));
  }
}
*/
