package stork.feather;

/**
 * A {@code Tap} emits {@link Slice}s to an attached {@link Sink}. The tap
 * should provide methods for regulating the flow of data (see {@link #pause()}
 * and {@link #resume()}) to allow attached sinks to prevent themselves from
 * being overwhelmed.
 * <p/>
 * {@code Tap}s may be either active or passive, as declared by the return
 * value of {@link #isActive()}. A passive {@code Tap} may be converted into an
 * active {@code Tap} through the use of a {@code Pump}.
 *
 * @see Sink
 * @see Slice
 */
public abstract class Tap extends PipeElement {
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
   * will be called and the tap may begin emitting {@link Slice}s.
   *
   * @param sink a {@link Sink} to attach.
   * @throws NullPointerException if {@code sink} is {@code null}.
   * @throws IllegalStateException is a sink has already been attached.
   */
  public final synchronized ProxyTransfer attach(Sink sink) {
    if (sink == null)
      throw new NullPointerException();
    if (transfer != null)
      throw new IllegalStateException("A sink is already attached.");
    return transfer = new ProxyTransfer(this, sink);
  }

  // Get the transfer, or throw an IllegalStateException if the transfer is not
  // ready.
  private final ProxyTransfer transfer() {
    if (transfer == null)
      throw new IllegalStateException();
    return transfer;
  }

  /**
   * Initialize the pipeline to transfer a {@code Resource}. Active {@code
   * Tap}s should call this to initialize downstream elements for transfer.
   * Passive taps will have this called to signal that a transfer should begin.
   */
  protected abstract Bell<?> initialize(RelativeResource resource);

  protected abstract void drain(RelativeSlice slice);

  protected abstract void finalize(RelativeResource resource);

  protected final void finalize(RelativeException error) {
    transfer().finalize(error);
  }

  protected final boolean random() {
    return transfer().random();
  }

  /**
   * 
   */
  protected abstract int concurrency();
}
