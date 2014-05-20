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
public abstract class Tap<R extends Resource> extends ProxyElement<R> {
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

  public final R source() { return root; }
  public final Resource destination() { return transfer().destination(); }

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
   * super.initialize(resource)}. The returned {@code Bell} will ring when the
   * attached {@code Sink} is ready to transfer data.
   * <p/>
   * Passive {@code Tap}s will have this called to indicate that a transfer
   * should begin, and must not call {@code super.initialize(...)}, or an
   * {@code IllegalStateException} will be thrown. A {@code Bell} must be
   * returned that will be rung when the {@code Sink} is ready to receive
   * {@code Slice}s from {@code resource}. Once the returned {@code Bell} has
   * been rung, and any asynchronous preparations in the {@code Tap} have
   * completed, the {@code Tap} may begin draining {@code Slice}s for {@code
   * resource}.
   *
   * @throws IllegalStateException if this {@code Tap} is passive and this
   * method has not been overridden.
   */
  public Bell<R> initialize(Relative<R> resource) {
    // This is kind of an antipattern, maybe we should have a subclass?
    if (!active) throw new
      IllegalStateException("Passive Taps must override initialize().");
    return transfer().initialize(resource);
  }

  /**
   * Initialize the transfer of data for the {@code Resource} specified by
   * {@code path} relative to the root {@code Resource}. This simply delegates
   * to {@link #initialize(Relative)}, and is made available as a convenience.
   *
   * @param path the path to the {@code Resource} which should be initialized
   * relative to the root.
   * @return A {@code Bell} which will ring when data for {@code path} is ready
   * to be drained, or {@code null} if data can begin being drained
   * immediately.
   */
  public final Bell<R> initialize(Path path) {
    return initialize(root.selectRelative(path));
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
   * Drain a {@link Slice} through the pipeline from the root {@code Resource}.
   * This delegates to {@link #drain(Relative)}.
   *
   * @param slice a {@code Slice} being drained through the pipeline.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public final void drain(Slice slice) {
    drain(root.wrap(slice));
  }

  /**
   * Drain a {@link Slice} through the pipeline for the {@code Resource} with the given
   * {@code Path}. This delegates to {@link #drain(Relative)}.
   *
   * @param path the path corresponding to the {@code Resource} the slice originated
   * from.
   * @param slice a {@code Slice} being drained through the pipeline.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public final void drain(Path path, Slice slice) {
    drain(root.wrap(path, slice));
  }

  /**
   * Drain a {@link Slice} through the pipeline for the given {@code
   * Relative<R>}. This delegates to {@link #drain(Relative)}.
   *
   * @param resource the {@code Resource} the slice originated from.
   * @param slice a {@code Slice} being drained through the pipeline.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public final void drain(Relative<R> resource, Slice slice) {
    drain(resource.wrap(slice));
  }

  /**
   * Finalize the transfer of a {@code Resource}. This method (or one of its
   * analogues) should be called by {@code Tap}s to indicate to the attached
   * {@code Sink} that a given {@code Resource} has finished transferring.
   * Non-data {@code Resource}s do not need to be finalized.
   */
  public final void finalize(Relative<R> resource) {
    transfer().finalize(resource);
  }

  /**
   * Finalize the transfer of data for the {@code Resource} specified by {@code
   * path}. This simply delegates to {@link #finalize(Relative)}.
   *
   * @param path the path to the {@code Resource} which should be finalized
   * relative to the root.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public final void finalize(Path path) {
    finalize(root.selectRelative(path));
  }
}
