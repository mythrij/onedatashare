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
  private Tap<S> tap;
  private Sink<D> sink;

  private Map<Path, Tap<S>>  taps  = new HashMap<Path, Tap<S>>();
  private Map<Path, Sink<D>> sinks = new HashMap<Path, Sink<D>>();

  private final Bell<Tap<S>> tapBell = new Bell<Tap<S>>() {
    public void done(Tap<S> tap) {
      ProxyTransfer.this.tap = tap;
      taps.put(Path.ROOT, tap);
    }
  };

  private final Bell<Sink<D>> sinkBell = new Bell<Sink<D>>() {
    public void done(Sink<D> sink) {
      ProxyTransfer.this.sink = sink;
      sinks.put(Path.ROOT, sink);
    }
  };

  private Bell ready = tapBell.and(sinkBell);

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
    sinkBell.ring(sink);
    return this;
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
    tapBell.ring(tap);
    return this;
  }

  private Tap<S> tapFor(Path path) {
    Tap<S> tap = taps.get(path);
    if (tap == null)
      throw new Error("Non-existent Tap.");
    return tap;
  }

  private Sink<D> sinkFor(Path path) {
    Sink<D> sink = sinks.get(path);
    if (sink == null)
      throw new Error("Non-existent Sink.");
    return sink;
  }

  // Called by tap.
  final void drain(Path path, Slice slice) {
    sinkFor(path).drain(slice);
  }

  final protected Bell<ProxyTransfer<S,D>> start() {
    return ready;
  }

  final protected void stop() {
    tap.finish();
    sink.finish();
  }

  final protected void pause(Bell bell) {
    tap.pause(bell);
  }

  /**
   * Called by {@code Tap} when a subresource has been discovered.
   */
  final protected void subresource(Path path) {
  }
}
