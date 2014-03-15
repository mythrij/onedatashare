package stork.feather;

/**
 * A handle on a remote resource, such as a file or directory. A resource
 * should essentially be a wrapper around a URI, and its creation should have
 * no side-effects. All of the methods specified by this interface should
 * return immediately. In the case of methods that return {@link Bell}s, the
 * result should be determined asynchronously and given to the caller through
 * the returned bell. Implementations that do not support certain operations
 * are allowed to throw an {@link UnsupportedOperationException}.
 *
 * @see Session
 * @see Stat
 * @see URI
 */
public class Resource {
  protected final Session session;
  protected final URI uri;

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
    this.session = ((Session) this).decorate();
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
    this.session = session.decorate();
  }

  /**
   * Reselect this resource through an equivalent session. Assuming the
   * sessions are actually equivalent, the returned resource will point to the
   * same physical resource. This can, for instance, be used to select the same
   * resource through an already existing session.
   *
   * @param session the session to reselect this resource on.
   * @return An equivalent resource which can be accessed through an equivalent
   * session.
   * @throws IllegalArgumentException if {@code session} is not equivalent to
   * this resource's session.
   * @throws NullPointerException if {@code session} is {@code null}.
   */
  public Resource reselect(Session session) {
    if (!session.equals(this.session))
      throw new IllegalArgumentException();
    return session.select(uri);
  }

  /**
   * Get the {@link Session) associated with this resource.
   *
   * @return The {@link Session} associated with this resource.
   * @see Session
   */
  public Session session() {
    return session;
  }

  /**
   * Get the {@link URI} associated with this resource.
   * 
   * @return A {@link URI} which identifies this resource.
   */
  public URI uri() {
    return uri;
  }

  /**
   * Get metadata for this resource, which includes a list of subresources.
   *
   * @return (via bell) A {@link Stat} containing resource metadata.
   * @throws ResourceException (via bell) if there was an error retrieving
   * metadata for the resource
   * @throws UnsupportedOperationException if metadata retrieval is not
   * supported
   */
  public Bell<Stat> stat() {
    return session.stat(uri);
  }

  /**
   * Create this resource as a directory on the storage system. If the resource
   * cannot be created, or already exists and is not a directory, the returned
   * {@link Bell} will be resolved with a {@link ResourceException}.
   *
   * @return (via bell) {@code null} if successful.
   * @throws ResourceException (via bell) if the directory could not be created
   * or already exists and is not a directory
   * @throws UnsupportedOperationException if creating directories is not
   * supported
   * @see Bell
   */
  public Bell<Void> mkdir() {
    return session.mkdir(uri);
  }

  /**
   * Delete the resource and all subresources from the storage system. If the
   * resource cannot be removed, the returned {@link Bell} will be resolved
   * with a ResourceException.
   *
   * @return (via bell) {@code null} if successful.
   * @throws ResourceException (via bell) if the resource could not be fully
   * removed
   * @throws UnsupportedOperationException if removal is not supported
   * @see Bell
   */
  public Bell<Void> rm() {
    return session.rm(uri);
  }

  /**
   * Select a subresource relative to this resource.
   *
   * @return A subresource relative to this resource.
   * @param path the path to the subresource, relative to this resource.
   * @see Bell
   */
  public final Resource select(String path) {
    if (path == null)
      return select((Path)null);
    return select(Path.create(path));
  }

  /**
   * Select a subresource relative to this resource.
   *
   * @return A subresource relative to this resource.
   * @param path the path to the subresource, relative to this resource.
   * @see Bell
   */
  public Resource select(Path path) {
    return session.select(uri.append(path));
  }

  /**
   * Called by client code to initiate a transfer to the named {@link Resource}
   * using whatever method is deemed most appropriate by the implementation.
   * The implementation should try to transfer the resource as efficiently as
   * possible, and so should inspect the destination resource to determine if
   * more efficient alternatives to proxy transferring can be done. This method
   * should perform a proxy transfer as a catch-all last resort.
   *
   * @param resource the destination resource to transfer this resource to
   * @return (via bell) A {@link Transfer} on success; the returned {@code
   * Transfer} object can be used to control and monitor the transfer.
   * @throws ResourceException (via bell) if the transfer fails
   * @throws UnsupportedOperationException if the direction of transfer is not
   * supported by one of the resources
   * @see Bell
   */
  public Bell<Transfer> transferTo(Resource resource) {
    return Transfer.proxy(tap(), resource.sink());
  }

  /**
   * Open a sink to the resource. Any connection operation, if necessary,
   * should begin as soon as this method is called. The returned bell should
   * be rung once the sink is ready to accept data.
   *
   * @return (via bell) A sink which drains to the named resource.
   * @throws ResourceException (via bell) if opening the sink fails
   * @throws UnsupportedOperationException if the resource does not support
   * writing
   * @see Bell
   */
  public Bell<Sink> sink() {
    return session.sink(uri);
  }

  /**
   * Open a tap on the resource. Any connection operation, if necessary, should
   * begin, as soon as this method is called. The returned bell should be rung
   * once the tap is ready to emit data.
   *
   * @return (via bell) A tap which emits slices from this resource and its
   * subresources.
   * @throws ResourceException (via bell) if opening the tap fails
   * @throws UnsupportedOperationException if the resource does not support
   * reading
   * @see Bell
   */
  public Bell<Tap> tap() {
    return session.tap(uri);
  }
}
