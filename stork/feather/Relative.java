package stork.feather;

/**
 * An object wrapper with contextual information regarding the object's origin
 * in some hierarchical transaction. This could be used in, for example, an
 * ongoing transfer or crawling operation. This wraps information such as the
 * {@code Resource} of origin (e.g., the {@code Resource} an object came from),
 * the root {@code Resource} in the context the originating {@code Resource} is
 * relative to (e.g., the root of the transfer), and the selection path of the
 * origin relative to the root. This relationship can be more simply thought of
 * as: {@code root.select(path).equals(origin)}
 *
 * @param <R> The type of {@code Resource} this {@code Relative} object
 * originated from.
 * @param <T> The type of the object wrapped by this {@code Relative} class.
 */
public class Relative<R,T> {
  /** The root {@code Resource} of the operation. */
  public final R root;

  /** The {@code Resource} that {@code object} originated from. */
  public final R origin;

  /** The selection {@code Path} from {@code root} to {@code origin}. */
  public final Path path;

  /** The wrapped object of type {@code T}. */
  public final T object;

  /**
   * Wrap {@code object} with the given {@code root} and {@code path}. The
   * {@code origin} will be selected automatically based on {@code root} and
   * {@code path}.
   *
   * @param object the {@code T} to wrap.
   * @param root the operational root.
   * @param path the {@code Path} from {@code root} to {@code origin}.
   */
  public Relative(T object, R root, Path path) {
    this(object, root, path, root.select(path));
  }

  /**
   * Wrap {@code object} with the given {@code root}, {@code path}, and {@code
   * origin}. This is intended to circumvent the slight overhead of selecting
   * {@code origin} from {@code root} and {@code path} if {@code origin} is
   * already known. However, this means it is possible to create a {@code
   * Relative} wrapper such that {@code !root.select(path).equals(origin)},
   * violating the class contract. Since this class is primarily used inside
   * this package, it shouldn't be a problem. If you're using it outside the
   * package, just play nice.
   *
   * @param object the {@code T} to wrap.
   * @param root the operational root.
   * @param path the {@code Path} from {@code root} to {@code origin}.
   * @param origin the operation origin of {@code object}.
   */
  public Relative(T object, R root, Path path, R origin) {
    this.root = root;
    this.path = path;
    this.object = object;
    origin = origin.select(path);
  }

  /**
   * Check if this object originated from the root.
   *
   * @return {@code true} if {@code path} is a root path; {@code false}
   * otherwise.
   */
  public boolean isRoot() {
    return path.isRoot();
  }
}
