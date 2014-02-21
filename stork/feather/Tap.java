package stork.feather;

/**
 * A tap emits {@link Slice}s to an attached {@link Sink}. The tap should
 * provide methods for regulating the flow of data (see {@link #cork()} and
 * {@link #uncork()}) to allow attached sinks to prevent themselves from being
 * overwhelmed.
 *
 * @see Resource
 * @see Sink
 * @see Slice
 */
public interface Tap {
  /**
   * Attach this tap to a {@link Sink}. One this method is called, the tap may
   * begin reading data from the upstream channel and emitting {@link Slice}s
   * to the sink. Until then, the tap should act as though it is corked.
   *
   * @param sink a {@link Sink} to attach
   */
  void attach(Sink sink);

  /**
   * Cork the tap, preventing slices from being emitted until {@link #uncork()}
   * is called. While corked, the tap should avoid reading from the upstream
   * channel, unless it buffers the data (which generally not advised). Calling
   * this method on a corked tap does nothing.
   */
  void cork();

  /**
   * Uncork the tap, resuming emission of slices and reading from the upstream
   * channel. Calling this method on an uncorked tap does nothing.
   */
  void uncork();
}
