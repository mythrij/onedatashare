package stork.feather;

/**
 * This is the base class for a passive {@code Tap} implementation.
 *
 * @see Sink
 * @see Slice
 */
public abstract class PassiveTap extends Tap {
  private ProxyTransfer transfer;

  /** The root resource of this tap. */
  public final Resource resource;

  /**
   * Create a {@code Tap} with the given {@code Resource} as the root resource.
   *
   * @param resource the {@code Resource} this {@code Tap} emits data from.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public PassiveTap(Resource resource) {
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

  protected final Bell<?> initialize(RelativeResource resource) {
    return transfer().initialize(resource);
  }

  protected final void drain(RelativeSlice slice) {
    transfer().drain(slice);
  }

  protected final void finalize(RelativeResource resource) {
    transfer().finalize(resource);
  }

  protected final void finalize(RelativeException error) {
    transfer().finalize(error);
  }

  protected final boolean random() {
    return transfer().random();
  }

  protected final int concurrency() {
    return transfer().concurrency();
  }
}
