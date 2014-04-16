package stork.feather.util;

/**
 * An anonymous {@code Tap} which emits the string representation of an object
 * via a {@link SliceTap}
 */
public class StringTap extends SliceTap {
  private final Slice slice;

  /**
   * Create a {@code SliceTap} which will emit only {@code slice}.
   *
   * @param slice the {@code Slice} to emit.
   */
  public SliceTap(Slice slice) {
    this.slice = slice;
  }

  public void start() {
    initialize(Path.ROOT).new Promise() {
      public void done() { drain(slice); }
    }
  }
}
