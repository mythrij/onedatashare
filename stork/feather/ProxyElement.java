package stork.feather;

/**
 * An abstract base class for anything which can serve as an element in a proxy
 * transfer pipeline. In particular, this is the base class for {@link Sink}
 * and {@link Tap}.
 *
 * @param <S> The source {@code Resource} type.
 * @param <D> The destination {@code Resource} type.
 */
public abstract class ProxyElement <S extends Resource, D extends Resource> {
  private S source;
  private D destination;
  private Path path = Path.ROOT;
  private ProxyTransfer<S,D> transfer;

  // Exactly one of these may be non-null.
  ProxyElement(S s, D d) {
    if (s == null && d == null)
      throw new NullPointerException("resource");
    if (s != null && d != null)
      throw new IllegalArgumentException();
    source = s;
    destination = d;
  }

  /**
   * The {@code Path} of this transfer element relative to the root of the
   * transfer.
   */
  public final Path path() {
    return path;
  } final void path(Path path) {
    this.path = path;
  }

  /**
   * Get the source {@code Resource} of the transfer.
   *
   * @throws IllegalStateException if a {@code Tap} has not been attached.
   */
  public final S source() {
    if (source == null)
      throw new IllegalStateException("No source has been set.");
    return source;
  } final synchronized void source(S source) {
    if (source != null)
      throw new IllegalStateException("A source has already been set.");
    this.source = source;
  }


  /**
   * Get the destination {@code Resource} of the transfer.
   *
   * @throws IllegalStateException if a {@code Sink} has not been attached.
   */
  public final D destination() {
    if (destination == null)
      throw new IllegalStateException("No destination has been set");
    return destination;
  } final synchronized void destination(D destination) {
    if (destination != null)
      throw new IllegalStateException("A destination has already been set.");
    this.destination = destination;
  }

  // Used in Tap and Sink to get a reference to the transfer.
  final synchronized ProxyTransfer<S,D> transfer() {
    if (transfer == null)
      throw new IllegalStateException("Not attached.");
    return transfer;
  } final synchronized ProxyTransfer<S,D> transfer(ProxyTransfer<S,D> t) {
    if (transfer != null)
      throw new IllegalStateException("Already attached.");
    return transfer = t;
  }

  /**
   * Drain a {@code Slice} through the pipeline. This method returns as soon as
   * possible, with the actual I/O operation taking place asynchronously.
   *
   * @param slice a {@code Slice} being drained through the pipeline.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  protected abstract void drain(Slice slice);

  /**
   * Prepare to begin the transfer of data. This involves, for example,
   * establishing data channels, pipelining transfer commands, altering state,
   * etc. The {@code Sink} will ring {@code bell} when data may begin being
   * drained.
   *
   * @param bell a {@code Bell} whose ringing indicates the transfer of data
   * may begin.
   * @throws Exception if this transfer element cannot be started for a reason
   * known immediately.
   */
  protected abstract void start(Bell bell) throws Exception;

  /** Finish the flow of data. */
  protected abstract void finish();

  /**
   * Pause the transfer until {@code Bell} is rung. When this is called, data
   * should no longer be drained through the pipeline, and the attached {@code
   * Tap} should cease reading from underlying data buffers. The {@code Sink}
   * will ring {@code bell} when the transfer of data may be resumed.
   *
   * @param bell the {@code Bell} whose ringing indicates that the transfer of
   * data may resume.
   */
  protected abstract void pause(Bell bell);

  /**
   * Free any system resources allocated by this transfer element. This will be
   * called after the transfer element has finished transferring data.
   * Subclasses should implement this to free any resources if any are
   * allocated.
   *
   * @throws Exception Any {@code Exception} thrown by this method is ignored.
   */
  protected void close() throws Exception { }
}
