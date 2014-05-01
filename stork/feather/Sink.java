package stork.feather;

/**
 * A sink is a destination for {@link Slice}s from other {@link Resource}s. It
 * is the sink's responsibility to "drain" the slice to the associated remote
 * resource (or data consumer). That is, data should be written as soon as
 * possible to the resource connection, and be retained only if necessary. Once
 * a slice is drained to the sink, it should not be assumed the slice can be
 * requested again, and it is the sink's responsibility to guarantee that the
 * slice is eventually drained.
 *
 * @see Tap
 * @see Slice
 */
public abstract class Sink extends PipeElement {
  private ProxyTransfer transfer;
  private final Resource resource;

  /**
   * Create a {@code Sink} with the given {@code Resource} as the root resource.
   *
   * @param resource the {@code Resource} this {@code Sink} receives data for.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Sink(Resource resource) {
    if (resource == null)
      throw new NullPointerException();
    this.resource = resource;
  }

  // Get the transfer, or throw an IllegalStateException if the transfer is not
  // ready.
  private final void transfer() {
    if (transfer == null)
      throw new IllegalStateException();
    return transfer;
  }

  public Resource root() { return resource; }

  /**
   * Attach this sink to a tap. Once this method is called, {@link #start()}
   * will be called and the sink may begin draining data from the tap. This is
   * equivalent to calling {@code tap.attach(this)}.
   *
   * @param tap a {@link Tap} to attach.
   * @throws NullPointerException if {@code tap} is {@code null}.
   * @throws IllegalStateException if a tap has already been attached.
   */
  public final ProxyTransfer attach(Tap tap) {
    if (tap == null)
      throw new NullPointerException();
    if (transfer != null)
      throw new IllegalStateException("A tap is already attached.");
    return tap.attach(this);
  }

  /**
   * This can be overridden by {@code Sink} implementations to initialize the
   * transfer of {@code Slice}s for a {@code RelativeResource}.
   */
  protected Bell<?> initialize(RelativeResource resource) {
    return null;
  }

  /**
   * This can be overridden by {@code Sink} implementations to finalize the
   * transfer of {@code Slice}s for a {@code RelativeResource}.
   */
  protected Bell<?> finalize(RelativeResource resource) {
    return null;
  }

  /**
   * {@code Sink} implementations can override this to handle initialization
   * {@code Exception}s from upstream. By default, this method will throw the
   * {@code Exception} back to the transfer mediator.
   *
   * @param path the path of the {@code Resource} which had an exception.
   * @throws Exception if {@code error} was not handled.
   */
  protected initialize(RelativeException error) throws Exception {
    throw error.getCause();
  }

  protected boolean random() { return false; }

  protected int concurrency() { return 1; }
  
  protected final boolean isActive() { return false; }

  /**
   * Called when an upstream tap encounters an error while downloading a {@link
   * Resource}. Depending on the nature of the error, the sink should decide to
   * either abort the transfer, omit the file, or take some other action.
   *
   * @param error the error that occurred during transfer, along with
   * contextual information
   */
  //void handle(ResourceException error);

  protected final void pause() {
    transfer().pause();
  }

  protected final void resume() {
    transfer().resume();
  }

  /**
   * Get the root {@code Resource} of the attached {@code Tap}.
   *
   * @return The root {@code Resource} of the attached {@code Tap}.
   * @throws IllegalStateException if a tap has not been attached.
   */
  public final Resource source() {
    return transfer().source();
  }

  /**
   * Get the {@code Resource} specified by {@code path} relative to the root of
   * the attached {@code Tap}.
   *
   * @return The {@code Resource} specified by {@code path} relative to the
   * root of the attached {@code Tap}.
   * @throws IllegalStateException if a tap has not been attached.
   */
  protected final Resource source(Path path) {
    return transfer.source(path);
  }
}
