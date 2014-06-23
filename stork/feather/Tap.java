package stork.feather;

/**
 * A {@code Tap} emits {@link Slice}s through a pipeline. {@code Tap}s should
 * provide methods for regulating the flow of {@code Slice}s to let downstream
 * to prevent the {@code Tap} from overwhelming them.
 * <p/>
 *
 * @see Sink
 * @see Slice
 *
 * @param <S> The source {@code Resource} type.
 */
public abstract class Tap<S extends Resource> extends Pipe {
  private Pipe sink;
  private S source;

  /**
   * Create a {@code Tap} associated with {@code source}.
   *
   * @param source the {@code Resource} this {@code Tap} emits data from.
   * @throws NullPointerException if {@code source} is {@code null}.
   */
  public Tap(S source) {
    if (source == null)
      throw new NullPointerException("source");
    this.source = source;
  }

  public final S source() { return source; }

  public final Tap<S> tap() { return this; }

  public final Pipe.Orientation orientation() {
    return Pipe.Orientation.TAP;
  }

  public final Resource destination() {
    if (sink == null)
      throw new IllegalStateException("Not attached.");
    return sink.destination();
  }

  /**
   * Start the flow of data from this {@code Tap}. Data may begin flowing once
   * {@code bell} rings.
   *
   * @param bell a {@code Bell} that rings when data may start flowing.
   * @return A {@code Bell} that rings when data has begun flowing.
   * @throws Exception if the transfer cannot be started for a reason known
   * immediately.
   */
  protected abstract Bell start(Bell bell) throws Exception;

  protected Bell drain(Slice slice) {
    try {
      Bell bell = super.drain(slice);
      if (bell == null)
        bell = Bell.rungBell();
      return bell;
    } catch (Exception e) {
      return new Bell(e);
    }
  }

  public final Bell start() {
    try {
      // Start downstream elements.
      Bell b1 = super.start();
      if (b1 == null) b1 = Bell.rungBell();
      // Start this element.
      Bell b2 = start(b1);
      if (b2 == null) b2 = Bell.rungBell();
      return b1.and(b2);
    } catch (Exception e) {
      return new Bell(e);
    }
  }
}
