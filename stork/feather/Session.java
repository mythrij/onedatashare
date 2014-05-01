package stork.feather;

import java.io.*;

/**
 * A {@code Session} represents a connection to a remote endpoint and all
 * associated configuration and state.
 *
 * @param <S> The equivalence class of this {@code Session} implementation.
 * Typically this will just be the implementation itself.
 */
public abstract class Session<S extends Session> extends Resource<S> {
  /** The authentication factor used for this endpoint. */
  protected final Credential credential;

  private final Bell<S> onOpen  = new Bell<S>();
  private final Bell<?> onClose = new Bell<?>();

  /**
   * Create a session with the given root URI.
   *
   * @param uri a {@link URI} representing the root of the session.
   */
  public Session(URI uri) {
    super(this);
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

  public final S session() {
    return this;
  }

  /**
   * Perform any operations necessary to prepare the {@code Session} to perform
   * an operation on the given {@code Resource}.
   *
   * @param resource the {@code Resource} to prepare to perform an operation
   * on.
   * @return A {@code Bell} which will ring with this {@code Session} when it
   * is prepared to perform an operation on {@code resource}.
   */
  public abstract Bell<? extends S> initialize(Resource<S> resource);

  /**
   * Perform any operations necessary to prepare the {@code Session} to perform
   * an operation on the given {@code Resource}.
   *
   * @param resource the {@code Resource} to prepare to perform an operation
   * on.
   * @return A {@code Bell} which will ring with this {@code Session} when it
   * is prepared to perform an operation on {@code resource}.
   */
  public abstract Bell<? extends S> finalize(Resource<S> resource);

  /**
   * Perform any operations necessary to prepare the {@code Session} to perform
   * an operation. This 
   * <p/>
   * Implementations that override this method must call {@code super.open()}
   * and return {@code super.onOpen()}.
   *
   * @return (via bell) Itself, once opening has completed. If this bell is
   * cancelled before the session has been established, the opening procedure
   * should be terminated.
   */
  public Bell<S> open() {
    return openBell.ring(this);
  }

  /**
   * Inform the session that it is no longer needed and should close any open
   * resources. This will also cancel the opening procedure if it has begun.
   * <p/>
   * Implementations that override this method must call {@code super.close()}
   * as the first line line of the method.
   * 
   * @return The result of {@link #onClose()}, which will have already been
   * rung.
   */
  public Bell<?> close() {
    onOpen
    return onClose.ring();
  }

  /**
   * Check if the closing procedure has begun.
   *
   * @return {@code true} if closing has begun; {@code false} otherwise.
   */
  public final synchronized boolean isClosed() {
    return onClose.isDone();
  }

  /**
   * Return a bell that will be rung when the {@code Session} is closed.
   *
   * @return A bell that will be rung when the {@code Session} is closed.
   */
  public final Bell<?> onClose() {
    return onClose.new Promise();
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
    return onClose
  }

  /**
   * If all references to this session have been lost, begin the closing
   * procedure.
   */
  protected final void finalize() { close(); }

  public String toString() {
    return uri.toString();
  }

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
