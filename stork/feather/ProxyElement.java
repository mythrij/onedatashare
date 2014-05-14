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
public abstract class ProxyElement<R extends Resource> {
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

  /**
   * Check if the pipeline is capable of draining {@code Slice}s in arbitrary
   * order. The return value of this method should remain constant across
   * calls.
   *
   * @return {@code true} if transmitting slices in arbitrary order is
   * supported.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public boolean random() { return false; }

  /**
   * Get the number of distinct {@code Resource}s the pipeline may be in the
   * process of transferring simultaneously. Specifically, this value limits
   * how many times {@code #initialize(...)} may be called before a
   * corresponding {@code #finalize(...)} must be called to free up a transfer
   * slot.
   * <p/>
   * Returning a number less than or equal to zero indicates that an arbitrary
   * number of {@code Resource}s may be transferred concurrently.
   *
   * @return The number of data {@code Resource}s this sink can receive
   * concurrently.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public int concurrency() { return 1; }

  /**
   * Start the flow of data.
   *
   * @return A {@code Bell} which will ring once the flow of data may begin.
   */
  protected Bell<?> start() { return new Bell().ring(); }

  /** Stop the flow of data permanently. */
  protected void stop() { }

  /** Pause the transfer. */
  protected void pause() { }

  /** Resume the the flow of data after a pause. */
  protected void resume() { }
}
