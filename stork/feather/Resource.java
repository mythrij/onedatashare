package stork.feather;

/**
 * A virtual representation of a physical resource. The resource(s) represented
 * by a {@code Resource} object may be a single resource (such as a file or
 * directory) or a set thereof (such as a directory tree or files matching a
 * pattern). The existence of a {@code Resource} object does not guarantee the
 * existence or accessibility of the resource(s) it represents.
 * <p/>
 * In general, a {@code Resource} represents a selection path from a top-level
 * {@code Resource} to one or many {@code Resource}s representing subresources.
 * This top-level {@code Resource} is known as the <i>root</i>. If a {@code
 * Resource} represents exactly one physical resource, it is called a
 * <i>singleton</i> {@code Resource}. The <i>trunk</i> of a {@code Resource} is
 * the first singleton {@code Resource} encountered when traversing backwards
 * to the root.
 * <p/>
 * A {@code Resource} instance may be selected through a {@code Session}, in
 * which case it is said to be <i>active</i>. Operations performed on an active
 * {@code Resource} are performed in the context of the {@code Session}. If a
 * {@code Resource} is not parented to a {@code Session}, the {@code Resource}
 * is said to be <i>inert</i>, and must be selected through a {@code Session}
 * to be operated on.
 * <p/>
 * All of the methods specified by this interface should return immediately. In
 * the case of methods that return {@link Bell}s, the result should be
 * determined asynchronously and given to the caller through the returned
 * {@code Bell}. Implementations that do not support certain operations are
 * allowed to throw an {@link UnsupportedOperationException}.
 *
 * @see Session
 * @see URI
 *
 * @param <S> The type of {@code Session} that can operate on this {@code
 * Resource}.
 */
public class Resource<S extends Session> {
  private Resource<S> parent;

  /**
   * The {@code Session} class calls this constructor.
   *
   * @param uri the URI referring to this resource.
   * @throws NullPointerException if {@code uri} is {@code null}.
   * @throws ClassCastException if the calling subclass is not a {@code
   * Session}.
   */
  protected Resource(URI uri) {
    if (uri == null)
      throw new NullPointerException();
    this.uri = uri.makeImmutable();
    this.session = ((Session) this);
  }

  /**
   * Create a resource with the same session and URI as another resource.
   *
   * @param resource the resource to make a copy of.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Resource(Resource resource) {
    this.uri = resource.uri;
    this.session = resource.session;
  }

  /**
   * Create a resource wrapping the given URI and session.
   *
   * @param session the session through which this resource can be accessed.
   * @param uri the URI referring to this resource.
   * @throws NullPointerException if either {@code session} or {@code uri} are
   * {@code null}.
   */
  public Resource(Session session, URI uri) {
    if (session == null || uri == null)
      throw new NullPointerException();
    this.uri = uri.makeImmutable();
    this.session = session;
  }

  /**
   * Get the {@link Session) this {@code Resource} is selected on.
   *
   * @return The {@link Session} associated with this resource, or {@code null}
   * if it is an inert {@code Resource}.
   */
  public S session() {
    return (parent == null) ? null : parent.session();
  }

  /**
   * Return the root {@code Resource} of this {@code Resource}. That is, the
   * {@code Resource} in the hierarchy that has no parent. For an active {@code
   * Resource}, this is the same as called {@link #session()}. For an inert
   * {@code Resource}, this will return the top-level {@code Resource}. The
   * root {@code Resource} is always a singleton.
   *
   * @return The root {@code Resource}.
   */
  public final Resource root() {
    return (parent == null) ? this : parent.root();
  }

  /**
   * Check if this {@code Resource} is active. That is, check that its root is
   * a {@code Session}. An active {@code Resource} may have operations
   * performed on it.
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
  public final Resource trunk() {
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
   * Get the sub-resources of this {@code Resource}. This can only be called on
   * a singleton {@code Resource}, and returns {@code null} if this {@code
   * Resource} cannot have sub-resources.
   */
  public Bell<Resource[]> subresources() {
    return stat().new PromiseAs<Resource[]>() {
      public Resource[] convert(Stat s) {
        if (s.files == null)
          return null;
        Resource[] r = new Resource[s.files.length];
        for (int i = 0; i < s.length; i++)
          r[i] = select(s.name);
        return r;
      }
    };
  }

  /**
   * Reselect this resource through an equivalent session. Assuming the
   * sessions are actually equivalent, the returned resource will point to the
   * same physical resource. This can, for instance, be used to select the same
   * resource through an already existing session.
   * <p/>
   * If this resource already belongs to the given session, this resource is
   * returned.
   *
   * @param session the session to reselect this resource on.
   * @return An equivalent resource which can be accessed through an equivalent
   * session.
   * @throws IllegalArgumentException if {@code session} is not equivalent to
   * this resource's session.
   * @throws NullPointerException if {@code session} is {@code null}.
   */
  public Resource reselectOn(S session) {
    if (session == this.session)
      return this;
    if (!session.equals(this.session))
      throw new IllegalArgumentException();
    return session.select(uri);
  }

  /**
   * Get a {@link URI} associated with this resource.
   * 
   * @return A {@link URI} which identifies this resource.
   */
  public URI uri() { return uri; }

  /**
   * Get metadata for this resource, which includes a list of subresources.
   *
   * @return (via bell) A {@link Stat} containing resource metadata.
   * @throws Exception (via bell) if there was an error retrieving
   * metadata for the resource
   * @throws UnsupportedOperationException if metadata retrieval is not
   * supported
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
  public Resource select(String name) {
    return new Resource(this, name);
  }

  /**
   * Select a subresource relative to this resource using the given path.
   *
   * @param path the path to the subresource, relative to this resource.
   * @return A subresource relative to this resource.
   */
  public final Resource select(Path path) {
    Resource r = this;
    for (String n : path.explode())
      r = r.select(n);
    return r;
  }

  /**
   * Select a subresource as a {@code RelativeResource} containing relative
   * path information. This is used during proxy transfers, and should
   * generally not be used in applications unless maintaining relative path
   * information across selection is necessary.
   *
   * @param path the path to the subresource, relative to this resource.
   * @return A subresource relative to this {@code Resource} containing a
   * reference to this {@code Resource}.
   */
  public final RelativeResource selectRelative(Path path) {
    if (this instanceof RelativeResource) {
      return new RelativeResource((RelativeResource) this, path);
    return new RelativeResource(this, path);
  }

  /**
   * Initiate a transfer from this {@code Resource} to {@code resource} using
   * whatever method is deemed most appropriate by the implementation.
   * The implementation should try to transfer the resource as efficiently as
   * possible, and so should inspect the destination resource to determine if
   * more efficient alternatives to proxy transferring can be done. This method
   * should perform a proxy transfer as a catch-all last resort.
   *
   * @param resource the destination resource to transfer this resource to
   * @return A {@link Transfer} on success. The returned {@code Transfer}
   * object can be used to control and monitor the transfer.
   * @throws UnsupportedOperationException if the direction of transfer is not
   * supported by one of the resources.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Transfer transferTo(Resource<?> resource) {
    return tap().attach(resource.sink());
  }

  /**
   * Open a sink to the resource. Any connection operation, if necessary,
   * should begin as soon as this method is called.
   *
   * @return A sink which drains to the named resource.
   * @throws Exception (via bell) if opening the sink fails.
   * @throws UnsupportedOperationException if the resource does not support
   * writing.
   */
  public Sink sink() { return session.sink(uri); }

  /**
   * Open a tap on the resource. Any connection operation, if necessary, should
   * begin, as soon as this method is called.
   *
   * @return (via bell) A tap which emits slices from this resource and its
   * subresources.
   * @throws Exception (via bell) if opening the tap fails.
   * @throws UnsupportedOperationException if the resource does not support
   * reading.
   */
  public Tap tap() { return session.tap(uri); }

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
