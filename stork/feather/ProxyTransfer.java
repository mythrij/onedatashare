package stork.feather;

import java.util.*;

/**
 * A mediator for a locally proxied data transfer. The transfer will begin
 * automatically once both a {@link Sink} and {@link Tap} are assigned.
 *
 * @param <S> the source {@code Resource} type.
 * @param <D> the destination {@code Resource} type.
 */
public class ProxyTransfer<S extends Resource, D extends Resource>
implements Transfer<S,D> {
  private Tap<S> tap;
  private Sink<D> sink;

  // Used to buffer excess slices.
  private List<Slice> buffer = new LinkedList<Slice>();

  private Bell<ProxyTransfer<S,D>> bell = new Bell<?>();

  private Bell<Sink> sinkBell = new Bell<Sink>() {
    protected void done(Sink sink) {
      setSink(sink);
    } protected void fail(Throwable t) {
      bell.ring(t);
    }
  };

  private Bell<Tap> tapBell = new Bell<Tap>() {
    protected void done(Tap tap) {
      setTap(tap);
    } protected void fail(Throwable t) {
      bell.ring(t);
    }
  };

  /**
   * Create a {@code ProxyTransfer} without a tap or sink set.
   */
  public ProxyTransfer() { }

  /**
   * Start a {@code ProxyTransfer} between {@code tap} and {@code sink}.
   *
   * @param tap the {@code Tap} to transfer from.
   * @param sink the {@code Sink} to transfer to.
   * @throws NullPointerException if {@code tap} or {@code sink} is {@code
   * null}.
   */
  public ProxyTransfer(Tap<S> tap, Sink<D> sink) {
    tap(tap).sink(sink);
  }

  // Internal methods which will set the sink/tap and check if the transfer can
  // start.
  private synchronized void setSink(Sink<D> sink) {
    if (sink == null)
      bell.ring(new NullPointerException());
    else
      this.sink = sink;
    if (tap != null)
      bell.ring(this);
    checkIfWeCanStart();
  } private synchronized void setTap(Tap<S> tap) {
    if (tap == null)
      bell.ring(new NullPointerException());
    else
      this.tap = tap;
    checkIfWeCanStart();
  }

  private final void checkIfWeCanStart() {
    if (tap != null && sink != null) {
      sink.start();
      tap.start();
      bell.ring(this);
    }
  }

  /**
   * Set the {@code Sink} for this transfer.
   *
   * @param sink the sink for this transfer.
   * @return This transfer.
   * @throws NullPointerException if {@code sink} is {@code null}.
   * @throws IllegalStateException if either {@code sink(...)} method has
   * already been called.
   */
  public final synchronized ProxyTransfer sink(Sink<D> sink) {
    if (sink == null)
      throw new NullPointerException();
    if (sinkBell == null)
      throw new IllegalStateException();
    sinkBell.ring(sink);
    return this;
  }

  /**
   * Asynchronously set the {@code Sink} for this transfer.
   *
   * @param sink the sink for this transfer.
   * @return This transfer.
   * @throws NullPointerException if {@code sink} is {@code null}.
   * @throws IllegalStateException if either {@code sink(...)} method has
   * already been called.
   */
  public final synchronized ProxyTransfer sink(Bell<Sink<D>> sink) {
    if (sink == null)
      throw new NullPointerException();
    if (sinkBell == null)
      throw new IllegalStateException();
    sink.promise(sinkBell);
    sinkBell = null;
    return this;
  }

  /**
   * Set the {@code Tap} for this transfer.
   *
   * @param tap the tap for this transfer.
   * @return This transfer.
   * @throws NullPointerException if {@code tap} is {@code null}.
   * @throws IllegalStateException if either {@code tap(...)} method has
   * already been called.
   */
  public final synchronized ProxyTransfer tap(Tap<S> tap) {
    if (tap == null)
      throw new NullPointerException();
    if (tapBell == null)
      throw new IllegalStateException();
    tapBell.ring(tap);
    return this;
  }

  /**
   * Asynchronously set the {@code Tap} for this transfer.
   *
   * @param tap the tap for this transfer.
   * @return This transfer.
   * @throws NullPointerException if {@code tap} is {@code null}.
   * @throws IllegalStateException if either {@code tap(...)} method has
   * already been called.
   */
  public final synchronized ProxyTransfer tap(Bell<Tap<S>> tap) {
    if (tap == null)
      throw new NullPointerException();
    if (tapBell == null)
      throw new IllegalStateException();
    tap.promise(tapBell);
    tapBell = null;
    return this;
  }

  /**
   * Get the root {@code Resource} of the attached {@code Tap}.
   *
   * @return The root {@code Resource} of the attached {@code Tap}.
   * @throws IllegalStateException if a tap has not been attached.
   */
  protected final S source() {
    if (tap == null)
      throw new IllegalStateException();
    return tap.root;
  }

  /**
   * Get the {@code Resource} specified by {@code path} relative to the root of
   * the attached {@code Tap}.
   *
   * @return The {@code Resource} specified by {@code path} relative to the
   * root of the attached {@code Tap}.
   * @throws IllegalStateException if a tap has not been attached.
   */
  protected final S source(Path path) {
    if (tap == null)
      throw new IllegalStateException();
    return tap.root;
  }

  // Called by tap.
  public Bell<?> initialize(Relative<Resource> r) {
    return sink.initialize(r);
  }

  // Called by tap.
  public void drain(Relative<Slice> slice) {
    sink.drain(slice);
  }

  // Called internally.
  public void start() {
    tap.start();
    sink.start();
  }

  // Called internally.
  public void stop() {
    tap.stop();
    sink.stop();
  }

  // Called by sink.
  public void pause() {
    tap.pause();
  }

  // Called by sink.
  public void resume() {
    tap.resume();
  }
}
