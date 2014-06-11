package stork.feather;

import stork.feather.util.*;

/**
 * A {@code Sink} is a destination for {@link Slice}s emitted by {@link Tap}s.
 * It is the {@code Sink}'s responsibility to "drain" {@code Slice}s to the
 * associated physical resource (or other data consumer). {@code Slice}s should
 * be drained as soon as possible to, and be retained only if necessary.
 * <p/>
 * If a {@code Slice} cannot be drained immediately, the {@code Sink} should
 * call {@code pause()} to prevent the attached {@code Tap} from emitting
 * further {@code Slice}s. Once a {@code Slice} is drained to the {@code Sink},
 * the {@code Slice} cannot be requested again, and it is the {@code Sink}'s
 * responsibility to guarantee that the {@code Slice} is eventually drained.
 *
 * @see Tap
 * @see Slice
 * @see Transfer
 *
 * @param <D> The destination {@code Resource} type.
 */
public abstract class Sink<D extends Resource>
extends ProxyElement<Resource,D> {
  /**
   * Create a {@code Sink} associated with {@code destination}.
   *
   * @param destination the {@code Resource} this {@code Sink} receives data
   * for.
   * @throws NullPointerException if {@code resource} is {@code null}.
   */
  public Sink(D destination) { super(null, destination); }

  /**
   * Attach this {@code Sink} to a {@code Tap}. Once this method is called,
   * {@link #start()} will be called and the sink may begin draining data from
   * the tap. This is equivalent to calling {@code tap.attach(this)}.
   *
   * @param tap a {@link Tap} to attach.
   * @throws NullPointerException if {@code tap} is {@code null}.
   * @throws IllegalStateException if a {@code Tap} has already been attached.
   */
  public final <S extends Resource<?,S>> Transfer<S,D> attach(Tap<S> tap) {
    return new ProxyTransfer<S,D>(tap, this);
  }

  protected void start(Bell bell) throws Exception {
    bell.ring();
  }

  /**
   * Drain a {@code Slice} to the endpoint storage system. This method returns
   * as soon as possible, with the actual I/O operation taking place
   * asynchronously.
   * <p/>
   * If the {@code Slice} cannot be drained immeditately due to congestion,
   * {@code pause()} should be called, and {@code resume()} should be called
   * when the channel is free to transmit data again.
   *
   * @param slice a {@code Slice} being drained through the pipeline.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  protected abstract void drain(Slice slice);

  protected abstract void finish();

  /**
   * This calls {@link #pause(Bell)} with a new {@code Bell}, and return the
   * {@code Bell}. The returned {@code Bell} should be rung once the transfer
   * of data may resume.
   *
   * @return A {@code Bell} that, when rung, resumes the transfer.
   */
  protected final Bell pause() {
    Bell bell = new Bell();
    pause(bell);
    return bell;
  }

  protected final void pause(Bell bell) {
    transfer().mediator.pause();
  }
}
