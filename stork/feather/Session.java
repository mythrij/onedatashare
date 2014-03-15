package stork.feather;

import java.io.*;

/**
 * A session represents a connection to a remote endpoint and all associated
 * configuration and state. {@code Session}s are stateful and, once opened,
 * represent an established connection to an endpoint. Sessions should not be
 * interacted with directly, and instead should have all operations performed
 * on them through through their decorators which enforce the Session-Client
 * Contract.
 * <p/>
 * The Session-Client Contract makes a number of guarantees with the aim of
 * simplifying the lives of both implementors and clients. The decorator
 * provided by this class spares clients and implementors from writing
 * excessive boilerplate to uphold the contract. The only requirement is that
 * sessions are accessed through the session decorator returned by {@link
 * #decorate()} in client code. The Session-Client Contract allows clients and
 * implementors to make the following assumptions:
 * <ol><li>
 *   Clients may call {@link #open()} multiple times, with subsequent calls
 *   having no effect and always returning a reference to the same bell.
 *   Implementors should assume {@link #open()} will only ever be called once.
 * </li><li>
 *   Clients may perform operations through the session without explicitly
 *   calling {@link #open()}. Implementors should assume {@link #open()} will
 *   have already been called before any operations begin.
 * </li><li>
 *   Clients may call {@link #close()} multiple times, with subsequent calls
 *   having no effect. Implementors should assume {@link #cleanup()} will be
 *   called at most once, the first time {@link #close()} is called.
 * </li></ol>
 */
public abstract class Session extends Resource {
  /** The authentication factor used for this endpoint. */
  public final Credential credential;

  // Ring this with a IOException when it's time to close.
  private final Bell<Object> doCloseBell = new Bell<Object>() {
    public void always() { cleanup(); }
  };

  private SessionDecorator decorator;

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

  // Constructor used solely by SessionDecorator.
  private Session(Session other) {
    this(other.uri, other.credential);
    decorator = (SessionDecorator) this;
  }

  /**
   * Return this session wrapped in a decorator which enforces the
   * Session-Client Contract. Using a session directly without wrapping it in a
   * decorator is considered to be a programmer error. Subsequent calls to this
   * method return the same object.
   *
   * @return this session wrapped in a decorator.
   */
  public final Session decorate() {
    if (decorator == null)
      decorator = new SessionDecorator(this);
    return decorator;
  }

  // A decorator which upholds the Session-Client Contract. We give ourselves
  // CTS writing boilerplate so you don't have to!
  private static class SessionDecorator extends Session {
    private final Session session;
    private Bell<Void> openBell = null;

    public SessionDecorator(Session s) {
      super(s);
      session = s;
    }

    // This really means "opening has definitely completed". This may return
    // true even if the session is closed, and opening may be in process even
    // if this returns false.
    private synchronized boolean isOpen() {
      return openBell != null && openBell.isDone();
    }

    // Simple helper class so we don't have TOO much boilerplate. Basically
    // proxies for another bell from an operation called after open is
    // complete.
    private abstract class OnOpen<T> extends Bell<T> {
      OnOpen() {
        open().promise(new Bell<Void>() {
          protected void done() {
            onClose(task().promise(OnOpen.this));
          } protected void fail(Throwable t) {
            OnOpen.this.ring(t);
          }
        });
      } abstract Bell<T> task();
    }

    public Bell<Stat> stat(final URI uri) {
      if (isClosed()) return (Bell<Stat>) onClose();
      if (isOpen())   return onClose(session.stat(uri));
      return new OnOpen<Stat>() {
        Bell<Stat> task() { return session.stat(uri); }
      };
    }

    public Bell<Void> mkdir(final URI uri) {
      if (isClosed()) return (Bell<Void>) onClose();
      if (isOpen())   return onClose(session.mkdir(uri));
      return new OnOpen<Void>() {
        Bell<Void> task() { return session.mkdir(uri); }
      };
    }

    public Bell<Void> rm(final URI uri) {
      if (isClosed()) return (Bell<Void>) onClose();
      if (isOpen())   return onClose(session.rm(uri));
      return new OnOpen<Void>() {
        Bell<Void> task() { return session.rm(uri); }
      };
    }

    public Bell<Sink> sink(final URI uri) {
      if (isClosed()) return (Bell<Sink>) onClose();
      if (isOpen())   return onClose(session.sink(uri));
      return new OnOpen<Sink>() {
        Bell<Sink> task() { return session.sink(uri); }
      };
    }

    public Bell<Tap> tap(final URI uri) {
      if (isClosed()) return (Bell<Tap>) onClose();
      if (isOpen())   return onClose(session.tap(uri));
      return new OnOpen<Tap>() {
        Bell<Tap> task() { return session.tap(uri); }
      };
    }

    public synchronized Bell<Void> open() {
      if (openBell != null)
        return openBell;
      openBell = session.open();
      if (openBell == null)  // Some beephead beeped up big time...
        close("Programmer error in open procedure.");
      return openBell;
    }

    public boolean equals(Object o) { return session.equals(o); }

    public int hashCode() { return session.hashCode(); }
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
  public Bell<Void> open() { return new Bell<Void>().ring(); }

  /**
   * Subclasses should override this to start the cleanup procedure during
   * closing. Cleanup should be performed asynchronously and this method should
   * return immediately.
   */
  public void cleanup() { }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources. This will also cancel the opening procedure if it has begun.
   */
  public final void close() { close(null, null); }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources, using {@code reason} as the explanation for closing. This will
   * also cancel the opening procedure if it has begun.
   *
   * @param reason the reason for closing this session.
   */
  public final void close(String reason) { close(reason, null); }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources, using {@code cause} as the cause for closing. This will
   * also cancel the opening procedure if it has begun.
   *
   * @param cause the exception that caused this session to close.
   */
  public final void close(Throwable cause) { close(null, cause); }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources, using {@code reason} as the explanation for closing and {@code
   * cause} as the cause for closing. This will also cancel the opening
   * procedure if it has begun.
   *
   * @param reason the reason for closing this session.
   * @param cause the exception that caused this session to close.
   */
  public final void close(String reason, Throwable cause) {
    doCloseBell.ring(new IOException(reason, cause));
  }

  /**
   * Check if the closing procedure has begun.
   *
   * @return {@code true} if closing has begun; {@code false} otherwise.
   */
  public synchronized boolean isClosed() {
    return doCloseBell.isDone();
  }

  /**
   * Return a bell that will be rung with a {@code IOException} when
   * the session is closed. This is the same is calling {@code onClose(new
   * Bell<T>())}.
   *
   * @return The value passed in for {@code bell}.
   * @throws IOException (via bell) when the session has begun closing.
   */
  public final Bell<?> onClose() {
    return onClose(new Bell());
  }

  /**
   * Register a bell to be rung with a {@code IOException} when the
   * session has begun its closing procedure.
   *
   * @return The value passed in for {@code bell}.
   * @param bell the bell to ring with a {@code IOException} when
   * the session is closed.
   * @throws IOException (via {@code bell}) when the session has
   * begun closing.
   */
  public final <T> Bell<T> onClose(Bell<T> bell) {
    final Bell<Object> b = (Bell<Object>) bell;
    return (b != null) ? (Bell<T>) doCloseBell.promise(b) : null;
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
