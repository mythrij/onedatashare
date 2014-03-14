package stork.feather;

/**
 * A session represents a connection to a remote endpoint and all associated
 * configuration and state. {@code Session}s are stateful and, once opened,
 * represent an established connection to an endpoint.
 */
public abstract class Session extends Resource {
  /** The authentication factor used for this endpoint. */
  public final Credential credential;

  private final Bell<Void> onClose = new Bell<Void>();
  private boolean closed = false;

  /**
   * Create a session with the given root URI.
   *
   * @param uri a {@link URI} representing the root of the session.
   */
  public Session(URI uri) {
    this(uri, null);
  }

  /**
   * Create a session with the given root URI and credential.
   *
   * @param uri a {@link URI} representing the root of the session.
   * @param credential a {@link Credential} used to authenticate with the
   * endpoint. May be {@code null} if no additional authentication factors are
   * required.
   * @throws NullPointerException if {@code uri} is {@code null}.
   */
  public Session(URI uri, Credential credential) {
    super(uri);
    this.credential = credential;
  }

  /**
   * Select a {@link Resource} from the session, based on a path.
   *
   * @param uri the URI of the resource to be selected, as a string.
   * @return The {@link Resource} identified by the URI.
   */
  public final Resource select(String uri) {
    return select(URI.create(uri));
  }

  /**
   * Select a {@link Resource} from the session, based on a URI.
   *
   * @param uri the URI of the resource to be selected
   * @return The {@link Resource} identified by the URI.
   */
  public Resource select(URI uri) {
    return new Resource(this, uri);
  }

  /**
   * Get metadata for the given URI, which includes a list of subresources.
   *
   * @param uri the URI of the resource to stat.
   * @return (via bell) A {@link Stat} containing resource metadata.
   * @throws ResourceException (via bell) if there was an error retrieving
   * metadata for the resource
   * @throws UnsupportedOperationException if metadata retrieval is not
   * supported
   */
  public Bell<Stat> stat(URI uri) {
    throw new UnsupportedOperationException();
  }

  /**
   * Create the resource specified by the given URI as a directory. If the
   * resource cannot be created, or already exists and is not a directory, the
   * returned {@link Bell} will be resolved with a {@link ResourceException}.
   *
   * @param uri the URI of the resource to create as a directory.
   * @return (via bell) {@code null} if successful.
   * @throws ResourceException (via bell) if the directory could not be created
   * or already exists and is not a directory
   * @throws UnsupportedOperationException if creating directories is not
   * supported
   * @see Bell
   */
  public Bell<Void> mkdir(URI uri) {
    throw new UnsupportedOperationException();
  }

  /**
   * Delete the resource specified by a URI and all subresources from the
   * storage system. If the resource cannot be removed, the returned {@link
   * Bell} will be resolved with a ResourceException.
   *
   * @param uri the URI of the resource to remove.
   * @return (via bell) {@code null} if successful.
   * @throws ResourceException (via bell) if the resource could not be fully
   * removed
   * @throws UnsupportedOperationException if removal is not supported
   * @see Bell
   */
  public Bell<Void> rm(URI uri) {
    throw new UnsupportedOperationException();
  }

  /**
   * Open a sink to the resource at the specified URI. Any connection
   * operation, if necessary, should begin as soon as this method is called.
   * The returned bell should be rung once the sink is ready to accept data.
   *
   * @param uri the URI of the resource to open a sink to.
   * @return (via bell) A sink which drains to the named resource.
   * @throws ResourceException (via bell) if opening the sink fails
   * @throws UnsupportedOperationException if the resource does not support
   * writing
   * @see Bell
   */
  public Bell<Sink> sink(URI uri) {
    throw new UnsupportedOperationException();
  }

  /**
   * Open a tap on the resource at the specified URI. Any connection operation,
   * if necessary, should begin, as soon as this method is called. The returned
   * bell should be rung once the tap is ready to emit data.
   *
   * @param uri the URI of the resource to open a tap on.
   * @return (via bell) A tap which emits slices from this resource and its
   * subresources.
   * @throws ResourceException (via bell) if opening the tap fails
   * @throws UnsupportedOperationException if the resource does not support
   * reading
   * @see Bell
   */
  public Bell<Tap> tap(URI uri) {
    throw new UnsupportedOperationException();
  }

  /**
   * Begin the connection and authentication procedure with the endpoint
   * system. Subsequent calls to this method should return the same bell.
   *
   * @return (via bell) {@code null}, once opening has completed. If this bell
   * is cancelled before the session has been established, the opening
   * procedure should be terminated.
   */
  public abstract Bell<Void> open();

  /**
   * Subclasses should override this to start the closing procedure. This
   * should return immediately, disallowing any further interaction, and begin
   * the closing procedure asynchronously. The cleanup should try to happen as
   * quickly and quietly as possible.
   */
  protected void doClose() { }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources. Multiple calls to this method have no effect.
   */
  public final void close() {
    if (closed) return;
    synchronized (this) {
      if (closed) return;  // Double-check. Race condition thing.
      closed = true;
    }
    onClose.ring();
    doClose();
  }

  /**
   * Check if the closing procedure has begun.
   *
   * @return {@code true} if closing has begun; {@code false} otherwise.
   */
  public boolean isClosed() {
    return closed;
  }

  /**
   * Get a {@link Bell} which will ring when the session closing procedure has
   * begun. I.e., it should ring when either {@link #close()} has been called
   * or the session has been terminated by some external activity. This can be
   * used to register handlers that will execute when closing has begun.
   * Subclasses should never ring this directly, as it is handled by {@link
   * #close()}.
   *
   * @return (via bell) {@code null}, once closing has completed. The bell has
   * no effect if cancelled, and the closing procedure cannot be cancelled once
   * started.
   */
  public final Bell<Void> onClose() {
    return onClose;
  }

  /**
   * If all references to this session have been lost, begin the closing
   * procedure.
   */
  protected final void finalize() { close(); }

  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Session)) return false;
    Session s = (Session) o;
    if (!uri.equals(s.uri))
      return false;
    if (credential == null)
      return s.credential == null;
    return credential.equals(s.credential);
  }

  public int hashCode() {
    return 1 + 13*uri.hashCode() +
           (credential != null ? 17*credential.hashCode() : 0);
  }
}
