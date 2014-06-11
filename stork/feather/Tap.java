package stork.feather;

/**
 * A {@code Tap} emits {@link Slice}s to an attached {@link Sink}. The {@code
 * Tap} should provide methods for regulating the flow of {@code Slice}s (see
 * {@link #pause()} and {@link #resume()}) to allow attached {@code Sink}s to
 * prevent themselves from being overwhelmed.
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
 *
 * @see Sink
 * @see Slice
 * @see Transfer
 *
 * @param <S> The source {@code Resource} type.
 */
public abstract class Tap<S extends Resource>
extends ProxyElement<S,Resource> {
  /**
   * Create a {@code Tap} associated with {@code source}.
   *
   * @param source the {@code Resource} this {@code Tap} emits data from.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Tap(S source) { super(source, null); }

  /**
   * Attach this {@cod Tap} to a {@code Sink}. Once this method is called,
   * {@link #start()} will be called and the {@code Tap} may begin emitting
   * {@link Slice}s.
   *
   * @param sink a {@link Sink} to attach.
   * @throws NullPointerException if {@code sink} is {@code null}.
   * @throws IllegalStateException if a {@code Sink} has already been attached.
   */
  public final <D extends Resource<?,D>> Transfer<S,D> attach(Sink<D> sink) {
    return new ProxyTransfer<S,D>(this, sink);
  }

  final void subattach(Path path, ProxyTransfer<S,Resource> transfer) {
    transfer(transfer);
  }

  /**
   * Declare the existence of a sub-{@code Resource}. The {@code Resource} is
   * named {@code name}, and its {@code Path} in the transfer is given by
   * {@code path().appendLiteral(name)}.
   */
  protected final void subresource(String name) {
    if (!name.equals(".") && !name.equals(".."))
      subresource(path().appendLiteral(name));
  }

  /**
   * Declare the existence of a sub-{@code Resource}. The {@code Path} of the
   * {@code Resource} is given by {@code path}.
   */
  protected final void subresource(Path path) {
    transfer().subresource(path);
  }

  protected abstract void start(Bell bell) throws Exception;

  protected void drain(Slice slice) {
    transfer().drain(path(), slice);
  }

  protected void finish() { }

  /**
   * Check if this is an active {@code Tap}.
   *
   * @return {@code true} if this is an active {@code Tap}; {@code false}
   * otherwise.
   */
  public final boolean isActive() {
    return !(this instanceof PassiveTap);
  }
}
