package stork.feather;

import java.util.*;

/**
 * A base class for anything which can serve as an element in a proxy transfer
 * pipeline. In particular, this is the base class for {@link Sink} and {@link
 * Tap}.
 */
public class Pipe {
  private Pipe upstream, downstream;

  /** The orientation of a {@code Pipe} in a pipeline. */
  public static enum Orientation {
    AMBIGUOUS, TAP, SINK, CONNECTED
  };

  /** Create an unattached {@code Pipe}. */
  public Pipe() { }

  /**
   * Get the source {@code Resource} of the transfer.
   *
   * @throws IllegalStateException if a {@code Tap} has not been attached.
   */
  public Resource source() { return upstream().source(); }

  /**
   * Get the destination {@code Resource} of the transfer.
   *
   * @throws IllegalStateException if a {@code Sink} has not been attached.
   */
  public Resource destination() { return downstream().destination(); }

  /**
   * Get the {@code Tap} which this {@code Pipe} receives data from.
   *
   * @return The {@code Tap}, or {@code null} if none is attached.
   */
  public Tap tap() {
    return (upstream == null) ? null : upstream.tap();
  }

  /**
   * Get the {@code Sink} which this {@code Pipe} drains to.
   *
   * @return The {@code Sink}, or {@code null} if none is attached.
   */
  public Sink sink() {
    return (downstream == null) ? null : downstream.sink();
  }

  /**
   * Get the upstream {@code Pipe} attached to this {@code Pipe}.
   *
   * @return The upstream {@code Pipe}, or {@code null} if none is attached.
   */
  public final Pipe upstream() { return upstream; }

  /**
   * Get the downstream {@code Pipe} attached to this {@code Pipe}.
   *
   * @return The downstream {@code Pipe}, or {@code null} if none is attached.
   */
  public final Pipe downstream() { return downstream; }

  /**
   * Attach {@code pipe} to this {@code Pipe} according to their orientations.
   *
   * @param pipe the {@code Pipe} to attach.
   * @throws IllegalStateException if {@code pipe} cannot be attached to this
   * {@code Pipe} due to conflicting orientations.
   * @return The {@code pipe} being attached.
   */
  public Pipe attach(Pipe pipe) {
    Orientation o1 = orientation(), o2 = pipe.orientation();
    if (o1 == Orientation.CONNECTED || o2 == Orientation.CONNECTED) {
      // Ensure that they're not both connected pipes...
      throw new IllegalStateException("Pipe is already connected.");
    } if (o1 == o2) {
      // ...and that they're not both ambiguous or conflicting...
      throw new IllegalStateException("Conflicting oritentations.");
    } switch (o1) {
      // ...so it's guaranteed that this pipe...
      case TAP:
        return (pipe.upstream = this).downstream = pipe;
      case SINK:
        return (pipe.downstream = this).upstream = pipe;
    } switch (o2) {
      // ...or the other is either tap-oriented or sink-oriented.
      case TAP:
        return (pipe.downstream = this).upstream = pipe;
      case SINK:
        return (pipe.upstream = this).downstream = pipe;
      default:
        throw new Error("Pipe.attach failed");
    }
  }

  /**
   * Get the {@code Orientation} of this {@code Pipe} in the pipeline. If a
   * pipe is connected to just a {@code Tap}, it is tap-oriented. If a pipe is
   * connected to just a {@code Sink}, it is sink-oriented. If it has both a
   * {@code Tap} and {@code Sink}, it is connected. If it has neither, it is
   * ambiguously oriented.
   */
  public Orientation orientation() {
    Pipe tap = tap(), sink = sink();
    return (tap == null) ?
      (sink == null ? Orientation.AMBIGUOUS : Orientation.SINK):
      (sink != null ? Orientation.CONNECTED : Orientation.TAP);
  }

  /**
   * Start the flow of data through the pipeline. This method returns as soon
   * as possible. Preparation should be done asynchronously.
   *
   * @return A {@code Bell} that rings when data may start flowing.
   * @throws IllegalStateException if a {@code Sink} has not been attached.
   * @throws Exception if the transfer cannot be started for a reason known
   * immediately.
   */
  protected Bell start() throws Exception {
    try {
      Bell bell = downstream().start();
      if (bell == null)
        bell = Bell.rungBell();
      return bell;
    } catch (Exception e) {
      return new Bell(e);
    }
  }

  /**
   * Drain a {@code Slice} through the pipeline. This method returns as soon as
   * possible, with the actual I/O operation taking place asynchronously.
   *
   * @param slice a {@code Slice} being drained through the pipeline.
   * @return A {@code Bell} that rings when the pipeline is ready for more
   * data, or {@code null} if more data is ready to be drained immediately.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   * @throws Exception if this transfer element cannot be started for a reason
   * known immediately.
   */
  protected Bell drain(Slice slice) throws Exception {
    try {
      Bell bell = downstream().drain(slice);
      if (bell == null)
        bell = Bell.rungBell();
      return bell;
    } catch (Exception e) {
      return new Bell(e);
    }
  }

  /**
   * Finalize the flow of data through this {@code Pipe}.
   */
  protected void finish() {
    downstream().finish();
  }

  /**
   * Retrieve the pipeline as a {@code List}. This is intended to be used for
   * debugging purposes primarily, and the returned {@code List} is purely
   * informational. That is, modifying it does not affect the state of the
   * pipeline. This may change in the future, however.
   */
  public final List<Pipe> pipeline() {
    return new LinkedList<Pipe>() {{
      Pipe p = Pipe.this;
      while (p.upstream != null) {
        // Find the most upstream pipe.
        p = p.upstream;
      } do {
        // Add all the downstream pipes.
        add(p);
        p = p.downstream;
      } while (p != null);
    }};
  }

  public String toString() {
    return getClass().getSimpleName();
  }
}
