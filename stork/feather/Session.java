package stork.feather;

import java.io.*;

/**
 * A {@code Session} is a stateful context for operating on {@code Resource}s.
 * {@code Session}s serve as a nexus for all of the stateful information
 * related to operating on a particular class of {@code Resource}s, which may
 * include network connections, user credentials, configuration options, and
 * other state necessary for operating on the {@code Resource}s it supports.
 * <p/>
 * {@code Resource}s handled by the {@code Session} may be <i>selected</i>
 * through the {@code Session}, in which case all operations on the {@code
 * Resource} will be performed in the context of the {@code Session}. Whether
 * or not a particular {@code Resource} is capable of being handled by the
 * {@code Session} depends on the URI associated with the {@code Resource} in
 * question.
 * </p>
 * {@code Session}s maintain state information regarding whether or not they
 * are ready for operation, and can be tested for equality based the URI and
 * user credentials used to instantiate them. This makes {@code Session}s
 * suitable for caching and reusing. A {@code Resource} selected on an
 * unconnected {@code Session} can, for instance, be reselected through an
 * equivalent {@code Session} that is still "warm", allowing the {@code
 * Session} to be reused and the initialization overhead to be avoided.
 *
 * @see Resource
 *
 * @param <S> The type of the subclass of this {@code Session}. This
 * unfortunate redundancy exists solely to circumvent weaknesses in Java's
 * typing system.
 * @param <R> The supertype of all {@code Resource}s handled by this {@code
 * Session}.
 */
public abstract class Session<S extends Session, R extends Resource> {
  /** The root {@code Resource} of this {@code Session}. */
  protected final R root;

  /** The URI used to describe this {@code Session}. */
  protected final URI uri;

  /** The authentication factor used for this endpoint. */
  protected final Credential credential;

  // Rung on close. Avoid letting this leak out.
  private final Bell<S> onFinalize = new Bell<S>();

  // This mediator is used in this package only.
  final ResourceMediator mediator = new ResourceMediator();

  /**
   * Create a {@code Session} with the given root URI.
   *
   * @param uri a {@link URI} representing the root of the session.
   * @throws NullPointerException if {@code root} or {@code uri} is {@code
   * null}.
   */
  protected Session(R root, URI uri) { this(uri, null); }

  /**
   * Create a {@code Session} with the given root URI and {@code Credential}.
   *
   * @param root the root {@code Resource} of this {@code Session}.
   * @param uri a {@link URI} describing the {@code Session}.
   * @param credential a {@link Credential} used to authenticate with the
   * endpoint. This may be {@code null} if no additional authentication factors
   * are required.
   * @throws NullPointerException if {@code root} or {@code uri} is {@code
   * null}.
   */
  protected Session(R root, URI uri, Credential credential) {
    this.uri = uri;
    this.credential = credential;
  }

  /**
   * Return the root {@code Resource} of this {@code Session}.
   *
   * @return The root {@code Resource} of this {@code Session}.
   */
  public final R root() { return root; }

  /**
   * Used internally in this package to manage resource initialization state.
   */
  final class ResourceMediator {
    Map<R,Bell<S>> inits = new HashMap<R,Bell<S>>();

    // Initialize the session, then initialize the resource.
    synchronized Bell<R> initialize(final R res) {
      if (res == Session.this)
        return initResource(Session.this);
  
      final Bell<R> bell = new Bell<R>();
      initResource(Session.this).new Promise() {
        public void done() {
          initResource(res).promise(bell);
        } public void fail(Throwable t) {
          bell.ring(t);
        }
      };
      return bell;
    }

    // Initialize a resource if it hasn't been.
    synchronized Bell<S> initResource(R res) {
      Bell<S> bell = inits.get(res);

      if (bell != null) {
        return bell;
      } try {
        bell = initialize(res);
      } catch (Exception e) {
        bell = new Bell<S>().ring(e);
      } if (bell == null) {
        bell = new Bell<S>().ring(res);
      }

      inits.put(res, bell);
      return bell;
    }

    // Finalize a resource if it's been initialized.
    synchronized void finalize(R res) {
      Bell<R> bell = inits.remove(res);
      if (bell != null)
        bell.cancel();
      if (res == Session.this)
        onFinalize.ring();
    }
  }

  /**
   * Prepare the {@code Session} to perform operations on the given {@code
   * Resource}. The exact nature of this preparation varies from implementation
   * to implementation. The implementor may assume that this method will not
   * be called again for the given {@code Resource} until said {@code Resource}
   * has been finalized. If {@code resource} is this {@code Session}, this
   * method should initialize the {@code Session} for general use, including
   * performing any connection and authentication operations.
   * <p/>
   * Implementations may return {@code null} if {@code resource} does not
   * require any asynchronous initialization. This method may also throw an
   * {@code Exception} if {@code resource} cannot be initialized for some
   * reason. By default, this method returns {@code null}.
   *
   * @param resource the {@code Resource} to prepare to perform an operation
   * on.
   * @return A {@code Bell} which will ring with {@code resource} when this
   * {@code Session} is prepared to perform operations on {@code resource}, or
   * {@code null} if the {@code Resource} requires no initialization.
   */
  protected Bell<R> initialize(R resource) { return null; }

  /**
   * Release any resources allocated during the initialization of {@code
   * resource}. If {@code resource} is this {@code Session}, this method should
   * close any connections and finalize any ongoing transactions.
   *
   * @param resource the {@code Resource} to prepare to perform an operation
   * on.
   */
  protected void finalize(R resource) { }

  /**
   * Check if the closing procedure has begun.
   *
   * @return {@code true} if closing has begun; {@code false} otherwise.
   */
  public final synchronized boolean isFinalized() {
    return onFinalize.isDone();
  }

  /**
   * Return a bell that will be rung with this {@code Session} when the {@code
   * Session} is closed.
   *
   * @return A bell that will be rung when the {@code Session} is closed.
   */
  public final Bell<S> onFinalize() {
    return onFinalize.new Promise();
  }

  /**
   * Register a {@code Bell} to be rung with this {@code Session} when the
   * {@code Session} is closed. This is slightly more memory-efficient than
   * promising on the {@code Bell} returned by {@link #onFinalize()}, as it
   * gets promised to an internal {@code Bell} directly.
   *
   * @param bell a {@code Bell} that will be rung when the {@code Session} is
   * closed.
   * @return Whatever value was passed in for {@code bell}.
   */
  public final Bell<S> onFinalize(Bell<S> bell) {
    return onFinalize.promise(bell);
  }

  /**
   * Select a {@code Resource} relative to the root {@code Resource} of this
   * {@code Session}.
   *
   * @param path the selection path to the {@code Resource} being selected.
   */
  public final R select(Path path) {
    return path.isRoot() ? root() : select(path.up()).select(path.name());
  }

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
