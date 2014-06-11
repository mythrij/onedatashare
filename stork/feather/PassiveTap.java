package stork.feather;

/**
 * {@code PassiveTap}s expect to have {@code Resource}s explicitly requested by
 * some active requestor process. In that sense, {@code PassiveTap}s behave
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
 * @param <R> The source {@code Resource} type.
 */
public abstract class PassiveTap<R extends Resource<?,R>> extends Tap<R> {
  /**
   * Create a {@code PassiveTap} associated with {@code resource}.
   *
   * @param resource the {@code Resource} this {@code Tap} emits data from.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public PassiveTap(R source) { super(source); }

  protected abstract void start(Bell bell) throws Exception;
}
