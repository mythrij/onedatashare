package stork.feather;

import java.util.*;

/**
 * A mediator for a locally proxied data transfer. The transfer will begin
 * automatically once both a {@link Sink} and {@link Tap} are assigned.
 * <p/>
 * This class extends {@code Bell<Transfer>}, and will ring with itself once
 * the transfer has begun, or with an exception if either this transfer's sink
 * or tap were assigned asynchronously with a bell which failed.
 */
public class ProxyTransfer extends Bell<Transfer>
implements Transfer, ProxyEnd {
  private Sink sink;
  private Tap tap;

  private Set<Controller> sources = new HashSet<Controller>();

  // A tap-like object for queueing then draining slices.
  private class QueueTap extends LinkedList<Slice> implements Controller {
    public void start() {
    }
  }

  // Representation of the state of a path vis a vis the sink.
  private class PathState {
    boolean inited = false;
    QueueTap queue = new QueueTap();

    void drain() {
      
    }
  }

  private Map<Path, PathState> paths = new HashMap<Path, PathState>();

  // Used to buffer excess slices.
  private List<Slice> buffer = new LinkedList<Slice>();

  private Bell<Sink> sinkBell = new Bell<Sink>() {
    protected void done(Sink sink) {
      setSink(sink);
    } protected void fail(Throwable t) {
      ProxyTransfer.this.ring(t);
    }
  };

  private Bell<Tap> tapBell = new Bell<Tap>() {
    protected void done(Tap tap) {
      setTap(tap);
    } protected void fail(Throwable t) {
      ProxyTransfer.this.ring(t);
    }
  };

  // Register a source controller for pausing, etc.
  synchronized void registerSource(Controller controller) {
    sources.add(Controller.decorate(controller));
  }

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
  public ProxyTransfer(Tap tap, Sink sink) {
    tap(tap).sink(sink);
  }

  // Internal methods which will set the sink/tap and check if the transfer can
  // start.
  private synchronized void setSink(Sink sink) {
    if (sink == null)
      ring(new NullPointerException());
    else
      this.sink = sink;
    if (tap != null)
      ring(this);
    checkIfWeCanStart();
  } private synchronized void setTap(Tap tap) {
    if (tap == null)
      ring(new NullPointerException());
    else
      this.tap = tap;
    checkIfWeCanStart();
  }

  private final void checkIfWeCanStart() {
    if (tap != null && sink != null) {
      sink.start();
      tap.start();
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
  public final synchronized ProxyTransfer sink(Sink sink) {
    return sink(new Bell<Sink>().ring(sink));
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
  public final synchronized ProxyTransfer sink(Bell<Sink> sink) {
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
  public final synchronized ProxyTransfer tap(Tap tap) {
    return tap(new Bell<Tap>().ring(tap));
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
  public final synchronized ProxyTransfer tap(Bell<Tap> tap) {
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
  protected final Resource source() {
    if (tap == null)
      throw new IllegalStateException();
    return transfer.source();
  }

  /**
   * Get the {@code Resource} specified by {@code path} relative to the root of
   * the attached {@code Tap}.
   *
   * @return The {@code Resource} specified by {@code path} relative to the
   * root of the attached {@code Tap}.
   * @throws IllegalStateException if a tap has not been attached.
   */
  protected final Resource source(Path path) {
    if (transfer == null)
      throw new IllegalStateException();
    return transfer.source(path);
  }

  public Bell<?> initialize(Path path) {
    return sink.initialize(path);
  }

  public Bell<?> initialize(Path path) {
    return sink.initialize(path);
  }

  public void drain(Path path, Slice slice) {
    // If this 
    if (!hasSeen(path)) {
      initialize(path);
    }
    sink.drain(slice);
  }

  public void start() {
    tap.control.start();
    sink.control.start();
  }

  public void stop() {
    tap.control.stop();
    sink.control.stop();
  }

  public void pause() {
    tap.control.pause();
  }

  public void resume() {
    tap.control.resume();
  }
}
