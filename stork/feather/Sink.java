package stork.feather;

/**
 * A sink is a destination for {@link Slice}s from other {@link Resource}s. It
 * is the sink's responsibility to "drain" the slice to the associated remote
 * resource (or data consumer). That is, data should be written as soon as
 * possible to the resource connection, and be retained only if necessary. Once
 * a slice is drained to the sink, it should not be assumed the slice can be
 * requested again, and it is the sink's responsibility to guarantee that the
 * slice is eventually drained.
 *
 * @see Resource
 * @see Tap
 * @see Slice
 */
public interface Sink {
  /**
   * Called when an upstream tap emits a {@link Slice}. The sink should
   * attempt to drain the slice as soon as possible. If, after writing, the
   * sink can no longer drain slices (e.g., when the connection to the remote
   * resource is overwhelmed), it should call {@link Tap#cork()} on the tap the
   * slice came from, and call {@link Tap#uncork()} when more data can be
   * written. In case the tap is implemented incorrectly and does not obey the
   * semantics of {@link Tap#cork()} and {@link Tap#uncork()}, the sink should
   * retain any slices not drained and drain them when it is able to. The sink
   * should never discard a slice.
   * <p/>
   * An empty slice indicates that the associated resource has been fully
   * transferred and no further slices will arrive for that resource. The
   * entire transfer is complete when an empty slice from the tap's root
   * resource is passed to this method, after which the sink may begin
   * finalizing the transfer.
   *
   * @param data a slice of data coming from an attached tap
   */
  void drain(Slice data);

  /**
   * Check if this sink supports random data access.
   */

  /**
   * Called when an upstream tap encounters an error while downloading a {@link
   * Resource}. Depending on the nature of the error, the sink should decide to
   * either abort the transfer, omit the file, or take some other action.
   *
   * @param error the error that occurred during transfer, along with
   * contextual information
   */
  //void handle(ResourceException error);
}
