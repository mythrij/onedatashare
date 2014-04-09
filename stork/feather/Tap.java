package stork.feather;

/**
 * A tap emits {@link Slice}s to an attached {@link Sink}. The tap should
 * provide methods for regulating the flow of data (see {@link #pause()} and
 * {@link #resume()}) to allow attached sinks to prevent themselves from being
 * overwhelmed.
 *
 * @see Sink
 * @see Slice
 */
public abstract class Tap extends ProxyElement {
  private ProxyTransfer transfer;

  /** The root resource of this tap. */
  public final Resource resource;

  /**
   * Create a {@code Tap} with the given {@code Resource} as the root resource.
   *
   * @param resource the {@code Resource} this {@code Tap} emits data from.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Tap(Resource resource) {
    if (resource == null)
      throw new NullPointerException();
    this.resource = resource;
  }

  /**
   * Attach this tap to a sink. Once this method is called, {@link #start()}
   * will be called and the tap may begin reading data from the upstream
   * channel and emitting {@link Slice}s.
   *
   * @param sink a {@link Sink} to attach.
   * @throws NullPointerException if {@code sink} is {@code null}.
   * @throws IllegalStateException is a sink has already been attached.
   */
  public final synchronized ProxyTransfer attach(Sink sink) {
    if (sink == null)
      throw new NullPointerException();
    if (transfer != null)
      throw new IllegalStateException("a sink is already attached");
    transfer = new ProxyTransfer().tap(this).sink(sink);
  }

  // Get the transfer, or throw an IllegalStateException if the transfer is not
  // ready.
  private final void transfer() {
    if (transfer == null)
      throw new IllegalStateException();
    return transfer;
  }

  protected final Bell<?> initialize(RelativeResource resource) {
    return transfer().initialize(resource);
  }

  protected final void drain(RelativeSlice slice) {
    transfer().drain(slice);
  }

  protected final void finalize(RelativeResource resource) {
    transfer().finalize(resource);
  }

  protected final boolean random() {
    return transfer().random();
  }

  protected final int concurrency() {
    return transfer().concurrency();
  }
}
