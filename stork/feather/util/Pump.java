package stork.feather.util;

import stork.feather.*;

/**
 * A {@code Pump} is an adapter over a passive {@code Tap} allows it to be used
 * as an active {@code Tap} by using a {@code Crawler} to automatically
 * transfer all subresources.
 *
 * @param <R> The {@code Resource} type of the {@code Tap} this {@code Pump}
 * adapts.
 */
public abstract class Pump<R extends Resource> extends Tap<R> {
  private Tap<R> tap;

  /**
   * Create a {@code Pump} which operates on {@code tap}. {@code tap} must be a
   * passive {@code Tap}.
   *
   * @param tap the {@code Tap} this {@code Pump} is adapting.
   * @throws IllegalArgumentException if {@code tap} is active.
   */
  public Pump(Tap<R> tap) {
    super(tap.root, true);
    this.tap = tap;
  }
}
