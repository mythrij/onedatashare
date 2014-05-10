package stork.feather;

import java.util.*;

import stork.feather.util.*;

/**
 * A virtual representation of a physical resource. The resource(s) represented
 * by a {@code Resource} object may be a single resource (such as a file or
 * directory) or a set thereof (such as a directory tree or files matching a
 * pattern). The existence of a {@code Resource} object does not guarantee the
 * existence or accessibility of the resource(s) it represents. In particular,
 * creating a {@code Resource} object does not 
 * <p/>
 * A {@code Resource} instance may be selected through a {@code Session}, in
 * which case it is said to be <i>active</i>. Operations performed on an active
 * {@code Resource} are performed in the context of the associated {@code
 * Session}. If a {@code Resource} is not parented to a {@code Session}, the
 * {@code Resource} is said to be <i>inert</i>.  An inert {@code Resource} must
 * be selected through a {@code Session} to be operated on.
 * <p/>
 * A {@code Resource} should be thought of as a placeholder for or reference to
 * a physical resource, with a {@code Session} as the entity responsible for
 * providing access it to the concrete resource(s) it represents. A {@code
 * Resource} object should not have any allocation state associated with it.
 * Instead, such state should be maintained by the {@code Session} the {@code
 * Resource} is selected through.
 * <p/>
 * In general, a {@code Resource} represents a selection path from a top-level
 * {@code Resource} (typically a {@code Session}) to one or many {@code
 * Resource}s representing subresources.  This top-level {@code Resource} is
 * known as the <i>root</i>. If a {@code Resource} represents exactly one
 * physical resource, it is called a <i>singleton</i> {@code Resource}. The
 * <i>trunk</i> of a {@code Resource} is the first singleton {@code Resource}
 * encountered when traversing backwards to the root.
 * <p/>
 * All of the methods specified by this interface should return immediately. In
 * the case of methods that return {@link Bell}s, the result should be
 * determined asynchronously and given to the caller through the returned
 * {@code Bell}. Implementations that do not support certain operations are
 * allowed to throw an {@link UnsupportedOperationException}.
 *
 * @see Session
 *
 * @param <S> The type of {@code Session} that can operate on this {@code
 * Resource}.
 * @param <R> The type of {@code Resource}s this {@code Resource} can be
 * selected from or produce through selection. Generally, for a subclass, this
 * is the subclass itself.
 */
public abstract class Resource<S extends Session, R extends Resource> {
  private final String name;
  private final R parent;
  private final S session;

  // Create a resource with the given parent and name.
  protected Resource(String name, R parent) {
    if (name == null || parent == null)
      throw new NullPointerException();
    this.name = name;
    this.parent = parent;
    session = null;
  }

  // Used to create root {@code Resource}s.
  protected Resource(S session) {
    if (parent == null)
      throw new NullPointerException();
    name = null;
    parent = null;
    this.session = session;
  }

  /**
   * Get the {@link Session} this {@code Resource} is selected on.
   *
   * @return The {@link Session} associated with this resource, or {@code null}
   * if it is an inert {@code Resource}.
   */
  public S session() {
    return parent == null ? session : (S) parent.session();
  }

  /**
   * Get the name of this {@code Resource}. This may be {@code null} if this is
   * a root {@code Resource}.
   *
   * @return The name of this {@code Resource}.
   */
  public String name() { return name; }

  /**
   * Return the root {@code Resource} of this {@code Resource}. That is, the
   * {@code Resource} in the hierarchy that has no parent. For an active {@code
   * Resource}, this is the same as called {@link #session()}. For an inert
   * {@code Resource}, this will return the top-level {@code Resource}. The
   * root {@code Resource} is always a singleton.
   *
   * @return This {@code Resource}'s root {@code Resource}.
   */
  public final R root() {
    return parent == this ? this : parent.root();
  }

  /**
   * Return whether or not this is a root {@code Resource}.
   *
   * @return {@code true} if this is a root {@code Resource}; {@code false}
   * otherwise.
   */
  public final boolean isRoot() { return parent == this; }

  /**
   * Check if this {@code Resource} is active. That is, check that its root is
   * a {@code Session}. An active {@code Resource} may have operations
   * performed on it. This does not mean that the {@code Resource} is
   * initialized or that the resource is represents exists.
   *
   * @return {@code true} if the {@code Resource} is active; {@code false}
   * otherwise.
   */
  public final boolean isActive() { return session() != null; }

  /**
   * Check if this {@code Resource} is inert. This returns {@code true} if and
   * only if {@link #isActive()} returns {@code false}. An inert {@code
   * Resource} must be selected through a {@code Session} in order to have have
   * operations performed on it.
   *
   * @return {@code true} if the {@code Resource} is inert; {@code false}
   * otherwise.
   */
  public final boolean isInert() { return !isActive(); }

  /**
   * Return the trunk of this {@code Resource}. That is, the first singleton
   * {@code Resource} encountered when traversing back to the root. Or, in
   * other words, the first {@code Resource} such that all segments in the
   * selection {@code Path} are non-glob segments.
   *
   * @return The first singleton ancestor of this {@code Resource}.
   */
  public final R trunk() {
    return isSingleton() ? this : parent.trunk();
  }

  /**
   * Check if this is a singleton {@code Resource}. That is, a {@code Resource}
   * that identifies exactly one physical resource.
   *
   * @return {@code true} if this is a singleton {@code Resource}; {@code
   * false} otherwise.
   */
  public final boolean isSingleton() {
    return true;
  }

  /**
   * Get the sub-resources of this {@code Resource}. Specifically, this returns
   * a mapping from {@code Resource} names as {@code String}s to {@code
   * Resource}s. This can only be called on a singleton {@code Resource} and
   * returns {@code null} if it is not a singleton {@code Resource}. It may
   * also return {@code null} either from the method or through the returned
   * {@code Bell} if this {@code Resource} does not represent a collection
   * resource.
   *
   * @return (via bell) A mapping from names to {@code Resource}s that
   * represent sub-resources of this {@code Resource}.
   */
  public Bell<Map<String,R>> subresources() {
    return stat().new PromiseAs<Map<String,R>>() {
      public Map<String,Resource> convert(Stat s) {
        if (s.files == null)
          return null;
        Map<String,R> map = new HashMash<String,R>();
        for (Stat s : s.files)
          map.put(s.name, select(s.name));
        return map;
      }
    };
  }

  /**
   * Reselect this {@code Resource} through an equivalent {@code Session}.
   * Assuming {@code session} is actually equivalent to this {@code Resource}'s
   * {@code Session}, the returned {@code Resource} will refer to the same
   * physical resource. This can, for instance, be used to select the same
   * resource through an already established session.
   * <p/>
   * If this {@code Resource} is already selected through {@code session} (or
   * is {@code null} and this {@code Resource} is inert), this {@code Resource}
   * is returned as-is.
   * <p/>
   * If {@code resource} is {@code null}, an inert equivalent
   *
   * @param session the session to reselect this resource on.
   * @return An equivalent {@code Resource} which can be accessed through
   * {@code session}.
   * @throws IllegalArgumentException if {@code session} is not equivalent to
   * this {@code Resource}'s {@code Session}.
   */
  public R reselectOn(S session) {
    if (session == session())
      return this;
    if (session == null)
      return new Session();
    if (!session.equals(this.session))
      throw new IllegalArgumentException();
    return session.select(path());
  }

  /**
   * Get a {@link URI} associated with this resource.
   * 
   * @return A {@link URI} which identifies this resource.
   */
  public URI uri() {
    return session().uri().path(path());
  }

  /**
   * Wrap an object with a {@code Relative} with this {@code Resource} as the
   * root and origin.
   */
  public <T> Relative<T> wrap(T object) {
    return new Relative<T>(object, this, Path.ROOT);
  }

  /**
   * Wrap an object with a {@code Relative} with this {@code Resource} as the
   * root.
   */
  public <T> Relative<T> wrap(Path path, T object) {
    return new Relative<T>(object, this, path);
  }

  /**
   * Initialize the {@code Session} associated with this {@code Resource} to
   * perform operations on this {@code Resource}. Implementors should call this
   * themselves in the method body of any operation that requires {@code
   * Session} initialization (for example, a control channel connection) to
   * ensure that the associated {@code Session} is ready to operate.  This can
   * also be used outside the framework to "warm up" the {@code Resource} in
   * anticipation of future use. The returned {@code Bell} will ring when
   * initialization is complete, indicating that {@code Resource}
   * implementations are allowed to perform the operation through the {@code
   * Session}.
   * <p/>
   * This method has no effect if the {@code Resource} is inert, and returns a
   * {@code Bell} rung with {@code null}.
   *
   * @return (via bell) The associated {@code Session} once it has been
   * initialized to operate on this {@code Resource}, unless the {@code
   * Resource} is inert. If it is insert, this method returns a {@code Bell}
   * already rung with {@code null}.
   */
  public final Bell<S> initialize() {
    if (parent != null)
      return parent.initialize();
    if (session == null)
      return new Bell<S>().ring();
    return session.mediatedInitialize();
  }

  /**
   * Get metadata for this resource, which includes a list of subresources.
   *
   * @return (via bell) A {@link Stat} containing resource metadata.
   * @throws Exception (via bell) if there was an error retrieving
   * metadata for the resource.
   * @throws UnsupportedOperationException if metadata retrieval is not
   * supported.
   */
  public Bell<Stat> stat() {
    throw new UnsupportedOperationException();
  }

  /**
   * Create this resource as a directory on the storage system. If the resource
   * cannot be created, or already exists and is not a directory, the returned
   * {@link Bell} will be resolved with an {@link Exception}.
   *
   * @return (via bell) {@code null} if successful.
   * @throws Exception (via bell) if the directory could not be created or
   * already exists and is not a directory.
   * @throws UnsupportedOperationException if creating directories is not
   * supported.
   */
  public Bell mkdir() {
    throw new UnsupportedOperationException();
  }

  /**
   * Delete the {@code Resource} from the storage system. If the resource
   * cannot be removed, the returned {@link Bell} will be resolved with an
   * {@code Exception}.
   *
   * @return (via bell) {@code null} if successful.
   * @throws Exception (via bell) if the resource could not be fully removed.
   * @throws UnsupportedOperationException if removal is not supported.
   */
  public Bell unlink() {
    throw new UnsupportedOperationException();
  }

  /**
   * Select a subresource by name relative to this resource. By default, this
   * simply appends {@code name} to the URI of this resource. Subclasses may
   * override this to introduce special behavior.
   *
   * @param name the unescaped name of a subresource to select.
   * @return A subresource of this resource.
   */
  public abstract R select(String name);

  /**
   * Select a subresource relative to this resource using the given path.
   *
   * @param path the path to the subresource, relative to this resource.
   * @return A subresource relative to this resource.
   */
  public final R select(Path path) {
    root().select(path().append(path));
  }

  /**
   * Select a subresource as a {@code Relative<Resource>} containing relative
   * path information. This is used during proxy transfers, and should
   * generally not be used in applications unless maintaining relative path
   * information across selection is necessary.
   *
   * @param path the path to the subresource, relative to this resource.
   * @return A subresource relative to this {@code Resource} containing a
   * reference to this {@code Resource}.
   */
  public final Relative<Resource> selectRelative(Path path) {
    R r = select(path);
    return new Relative<Resource>(r, this, path, r);
  }

  /**
   * Get the selection path of this {@code Resource} relative to the root.
   *
   * @return The selection path of this {@code Resource} relative to the root.
   */
  public Path path() {
    return (name == null) ? Path.ROOT : parent.path().appendLiteral(name);
  }

  /**
   * Initiate a transfer from this {@code Resource} to {@code resource} using
   * whatever method is deemed most appropriate by the implementation.  The
   * implementation should try to transfer this {@code Resource} as efficiently
   * as possible, and so should inspect the destination {@code Resource} to
   * determine if more efficient alternatives to proxy transferring can be
   * done. This method should perform a proxy transfer as a catch-all last
   * resort.
   *
   * @param resource the destination resource to transfer this resource to
   * @return A {@link Transfer} on success. The returned {@code Transfer}
   * object can be used to control and monitor the transfer.
   * @throws UnsupportedOperationException if the direction of transfer is not
   * supported by one of the resources.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public <D extends Resource> Transfer<R,D> transferTo(D resource) {
    return tap().attach(resource.sink());
  }

  /**
   * Return a {@code Sink} that will drain data for this {@code Resource}. Any
   * connection operation, if necessary, should begin asynchronously as soon as
   * this method is called.
   *
   * @return A {@code Sink} which drains {@code Slice}s to this {@code
   * Resource}.
   * @throws Exception (via bell) if opening the {@code Sink} fails.
   * @throws UnsupportedOperationException if this {@code Resource} does not
   * support writing.
   */
  public Sink<R> sink() { return session.sink(uri); }

  /**
   * Return a {@code Tap} that will emit data from this {@code Resource}. Any
   * connection operation, if necessary, should begin asynchronously as soon as
   * this method is called.
   *
   * @return (via bell) A {@code Tap} which emits {@code Slice}s from this
   * {@code Resource}.
   * @throws Exception (via bell) if opening the {@code Tap} fails.
   * @throws UnsupportedOperationException if this {@code Resource} does not
   * support reading.
   */
  public Tap<R> tap() { return session.tap(uri); }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Resource)) return false;
    Resource r = (Resource) o;
    if (!uri.equals(r.uri))
      return false;
    return true;
  }

  public int hashCode() {
    return 13*uri.hashCode() + 17*session.hashCode();
  }
}
