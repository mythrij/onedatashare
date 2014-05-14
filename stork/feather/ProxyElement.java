package stork.feather;

/**
 * An abstract base class for anything which can serve as an element in a proxy
 * transfer pipeline. In particular, this is the base class for {@link Sink}
 * and {@link Tap}.
 * <p/>
 * {@code ProxyElement}s must implement behavior for controlling the flow of
 * data on demand and for initializing, draining to, and finalizing {@code
 * Resource}s handled by the {@code ProxyElement}.
 *
 * @param <R> The root {@code Resource} type.
 */
public abstract class ProxyElement<R extends Resource> implements Controller {
  /**
   * Prepare the pipeline for the transfer of data for {@code resource}. This
   * must be called before any data is drained for {@code resource}.
   * <p/>
   * This method returns immediately, and initialization takes place
   * asynchronously. It may return a {@code Bell} which will be rung when the
   * initialization process is complete and slices for {@code path} may begun
   * being drained through this endpoint. It may also return {@code null} to
   * indicate that transmission may begin immediately.
   *
   * @param resource the {@code Resource} which should be initialized, in a
   * {@code Relative} wrapper.
   * @return A {@code Bell} which will ring when data for {@code resource} is
   * ready to be drained, or {@code null} if data can begin being drained
   * immediately.
   * @throws Exception (via bell) if the {@code Resource} cannot be
   * initialized.
   */
  public abstract Bell<R> initialize(Relative<R> resource) throws Exception;

  /**
   * Drain a {@code Relative<Slice>} through the pipeline. This method
   * returns as soon as possible, with the actual I/O operation taking place
   * asynchronously.
   *
   * @param slice a {@code Slice} being drained through the pipeline.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public abstract void drain(Relative<Slice> slice);

  /**
   * Finalize the transfer of data for the specified {@code Resource}. This
   * method should return immediately, and finalization should take place
   * asynchronously.
   *
   * @param resource the {@code Resource} being finalized.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public abstract void finalize(Relative<R> resource);

  public Bell<?> start() { return null; }
  public void stop() { }
  public Bell<?> pause() { return null; }
  public void resume() { }
}
