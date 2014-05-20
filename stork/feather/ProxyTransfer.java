package stork.feather;

import java.util.*;

import stork.feather.util.*;

/**
 * A mediator for a locally proxied data transfer. The transfer will begin
 * automatically once both a {@link Sink} and {@link Tap} are assigned.
 *
 * @param <S> the source {@code Resource} type.
 * @param <D> the destination {@code Resource} type.
 */
public class ProxyTransfer<S extends Resource<?,S>, D extends Resource<?,D>>
extends Transfer<S,D> {
  private Tap<S> tap;
  private Sink<D> sink;

  // Used to buffer excess slices.
  private List<Slice> buffer = new LinkedList<Slice>();

  private Bell<ProxyTransfer<S,D>> bell = new Bell<ProxyTransfer<S,D>>();

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
    if (tap != null && sink != null) try {
      Bell<S> tb = tap.start();
      Bell<D> sb = sink.start();

      if (tb == null) tb = new Bell<S>().ring();
      if (sb == null) sb = new Bell<D>().ring();

      // Start a crawler if the tap is passive.
      if (!tap.active) tb.new Promise() {
        public void done() {
          new Crawler<S>(tap.root, true) {
            public void operate(Relative<S> resource) {
              tap.initialize(resource);
            }
          }.start();
        }
      };

      ((Bell)sb).and(tb).thenAs(this).promise(bell);
    } catch (Exception e) {
      bell.ring(e);
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
  public final S source() {
    if (tap == null)
      throw new IllegalStateException();
    return tap.root;
  }

  /**
   * Get the root {@code Resource} of the attached {@code Sink}.
   *
   * @return The root {@code Resource} of the attached {@code Sink}.
   * @throws IllegalStateException if a tap has not been attached.
   */
  public final D destination() {
    if (sink == null)
      throw new IllegalStateException();
    return sink.root;
  }

  // Called by tap.
  public Bell<S> initialize(Relative<S> r) {
    try {
      Bell<D> bell = sink.initialize((Relative<D>) r.wrap(sink.root));
      if (bell == null)
        return new Bell<S>().ring(r.object);
      return bell.new ThenAs<S>(r.object);
    } catch (Exception e) {
      return new Bell<S>().ring(e);
    }
  }

  // Called by tap.
  public void finalize(Relative<S> r) {
    try {
      sink.finalize(r.wrap((D) sink.root));
    } catch (Exception e) {
      // TODO: How should we handle this?
    }
  }

  // Called by tap.
  final void drain(Relative<Slice> slice) {
    sink.drain(slice);
  }

  final protected void stop() {
    tap.stop();
    sink.stop();
  }

  final protected void pause() {
    tap.pause();
  }

  final protected void resume() {
    tap.resume();
  }
}
