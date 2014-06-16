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
public class ProxyTransfer<S extends Resource, D extends Resource>
extends Transfer<S,D> {
  private Tap<S>  tap;
  private Sink<D> sink;

  private Bell ready = new Bell();

  private List<Slice> buffer = new LinkedList<Slice>();

  /**
   * Create a {@code ProxyTransfer} without a {@code Tap} or {@code Sink} set.
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

  public final S source() {
    if (tap == null)
      throw new IllegalStateException();
    return tap.source();
  }

  public final D destination() {
    if (sink == null)
      throw new IllegalStateException();
    return sink.destination();
  }

  /**
   * Set the {@code Tap} for this transfer.
   *
   * @param tap the {@code Tap} for this transfer.
   * @return This transfer.
   * @throws NullPointerException if {@code tap} is {@code null}.
   * @throws IllegalStateException if a {@code Tap} has already been set.
   */
  public final synchronized ProxyTransfer<S,D> tap(Tap<S> tap) {
    if (tap == null)
      throw new NullPointerException("tap");
    if (this.tap != null)
      throw new IllegalStateException("A Tap is already set.");
    if (sink != null)
      startTransfer();
    return this;
  }

  /**
   * Set the {@code Sink} for this transfer.
   *
   * @param sink the {@code Sink} for this transfer.
   * @return This transfer.
   * @throws NullPointerException if {@code sink} is {@code null}.
   * @throws IllegalStateException if a {@code Sink} has already been set.
   */
  public final synchronized ProxyTransfer<S,D> sink(Sink<D> sink) {
    if (sink == null)
      throw new NullPointerException("sink");
    if (this.sink != null)
      throw new IllegalStateException("A Sink is already set.");
    this.sink = sink;
    if (tap != null)
      startTransfer();
    return this;
  }

  private void startTransfer() {
    try {
      tap.attach(sink);
      tap.start();
    } catch (Exception e) {
      throw new RuntimeException("Transfer failed to start.", e);
    }
  }
}
