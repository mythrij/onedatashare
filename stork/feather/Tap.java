package stork.feather;

/**
 * A {@code Tap} emits {@link Slice}s to an attached {@link Sink}. The {@code
 * Tap} should provide methods for regulating the flow of {@code Slice}s (see
 * {@link #pause()} and {@link #resume()}) to allow attached {@code Sink}s to
 * prevent themselves from being overwhelmed.
 * <p/>
 * {@code Tap}s may either be <i>active</i> or <i>passive</i>. The distinction
 * lies in how the {@code Tap} manages {@code Resource} ordering and
 * initialization. Whether a {@code Tap} is active or passive influences the
 * semantics of {@code initialize(...)}.
 * <p/>
 * Active {@code Tap}s handle their own initialization and ordering of {@code
 * Resource}s, which are typically imposed by some external mechanism. These
 * {@code Tap}s will call {@code initialize(...)} themselves when a {@code
 * Resource} is ready for transfer. An example is an incoming multipart HTTP
 * request viewed as a {@code Tap}, where {@code initialize(...)} will be
 * called by the {@code Tap} itself for every incoming file. Active {@code
 * Tap}s may override {@link #initialize(Relative)} to implement custom
 * behavior, but must ultimately return the result of {@code
 * super.initialize(...)} to initialize the pipeline.
 * <p/>
 * Passive {@code Tap}s expect to have {@code Resource}s explicitly requested
 * by some active requestor process. In that sense, these {@code Tap}s behave
 * somewhat like a traditional data server. An example is an FTP session, where
 * the {@code Tap} waits for {@code initialize(...)} to be called on each
 * {@code Resource} when it is ready to be transferred. Passive {@code Tap}s
 * must override {@link #initialize(Relative)} to implement behavior that
 * starts the transfer of data for the requested {@code Resource}.
 * <p/>
 * A passive {@code Tap} can be converted into an active {@code Tap} through
 * the use of a {@code Pump} adapter. A {@code Pump} uses a {@code Crawler} to
 * traverse a collection {@code Resource}s and transfers each in turn. {@code
 * Pump}s will be transparently inserted into the pipeline when a passive
 * {@code Tap} is attached to a {@code Sink}.
 *
 * @see Sink
 * @see Slice
 */
public abstract class Tap<R extends Resource> extends PipeElement<R> {
  private ProxyTransfer<R,?> transfer;

  /** Whether or not this is an active {@code Tap}. */
  public final boolean active;

  /**
   * Create a {@code Tap} with an anonymous root {@code Resource}.
   */
  public Tap() { this((R) Resource.ANONYMOUS); }

  /**
   * Create a {@code Tap} with an anonymous root {@code Resource}.
   *
   * @param active whether or not this {@code Tap} is active.
   */
  public Tap(boolean active) { this((R) Resource.ANONYMOUS, active); }

  /**
   * Create an active {@code Tap} with {@code root} as the root {@code
   * Resource}.
   *
   * @param root the {@code Resource} this {@code Tap} emits data from.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Tap(R root) { this(root, false); }

  /**
   * Create a {@code Tap} with the given {@code Resource} as the root resource.
   *
   * @param root the {@code Resource} this {@code Tap} emits data from.
   * @param active whether or not this {@code Tap} is active.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Tap(R root, boolean active) {
    super(root);
    this.active = active;
  }

  /**
   * Attach this tap to a {@code Sink}. Once this method is called, {@link
   * #start()} will be called and the {@code Tap} may begin emitting {@link
   * Slice}s.
   *
   * @param sink a {@link Sink} to attach.
   * @throws NullPointerException if {@code sink} is {@code null}.
   * @throws IllegalStateException is a {@code Sink} has already been attached.
   */
  public final <D extends Resource> ProxyTransfer<R,D> attach(Sink<D> sink) {
    if (sink == null)
      throw new NullPointerException();
    synchronized (this) {
      if (transfer != null)
        throw new IllegalStateException("A Sink is already attached.");
      transfer = new ProxyTransfer<R,D>(this, sink);
      return (ProxyTransfer<R,D>) transfer;
    }
  }

  // Get the transfer, or throw an IllegalStateException if the transfer is not
  // ready.
  private synchronized final ProxyTransfer<R,?> transfer() {
    if (transfer == null)
      throw new IllegalStateException();
    return transfer;
  }

  /**
   * Initialize the pipeline to transfer a {@code Resource}. Active {@code
   * Tap}s should call this to initialize downstream elements for transfer.
   * Active subclasses may override this, but must return {@code
   * super.initialize(resource)}.
   * <p/>
   * Passive {@code Tap}s will have this called to indicate that a transfer
   * should begin. Passive {@code Tap}s must not call {@code
   * super.initialize(...)}, or an {@code IllegalStateException} will be
   * thrown.
   *
   * @throws IllegalStateException if this {@code Tap} is passive and this
   * method has not been overridden.
   */
  public Bell<R> initialize(Relative<R> resource) {
    // This is kind of an antipattern, maybe we should have a subclass?
    if (!active) throw new
      IllegalStateException("Passive Taps must override initialize().");
    return transfer.initialize(resource);
  }

  /**
   * Drain a {@code Slice} to the {@code Sink}. This method (or one of its
   * analogues) should be called by {@code Tap}s to drain {@code Slice}s
   * through the pipeline.
   */
  public final void drain(Relative<Slice> slice) {
    transfer().drain(slice);
  }

  /**
   * Finalize the transfer of a {@code Resource}. This method (or one of its
   * analogues) should be called by {@code Tap}s to finalize indicate to the
   * attached {@code Sink} that a given {@code Resource} has finished
   * transferring.
   *
   * @throws IllegalStateException if this {@code Tap} is passive and this
   * method has not been overridden.
   */
  public final void finalize(Relative<R> resource) {
    if (!active) throw new
      IllegalStateException("Passive Taps must override finalize().");
    transfer.finalize(resource);
  }
}
