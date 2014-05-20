package stork.feather;

import stork.feather.util.Crawler;

/**
 * A {@code Pump} is an adapter over a passive {@code Tap} allows it to be used
 * as an active {@code Tap} by using a {@code Crawler} to automatically
 * transfer all subresources.
 *
 * @param <R> The {@code Resource} type of the {@code Tap} this {@code Pump}
 * adapts.
 */
public class Pump<R extends Resource<?,R>> extends Tap<R> {
  private final Tap<R> tap;

  /**
   * Create a {@code Pump} which operates on {@code tap}. {@code tap} must be a
   * passive {@code Tap}.
   *
   * @param tap the {@code Tap} this {@code Pump} is adapting.
   * @throws IllegalArgumentException if {@code tap} is active.
   */
  public Pump(Tap<R> tap) {
    super(tap.root, true);
    if (tap.active)
      throw new RuntimeException("Cannot attach a Pump to an active Tap");
    this.tap = tap;
  }

  public int concurrency() { return tap.concurrency(); }
  public boolean random()  { return tap.random(); }

  public void pause()  { tap.pause(); }
  public void stop()   { tap.stop(); }
  public void resume() { tap.resume(); }

  /**
   * Start the attached {@code Tap} then initialize a {@code Crawler} which
   * will transfer everything under {@code root}.
   */
  public Bell<R> start() {
    return tap.start().new Promise() {
      public void done() {
        new Crawler<R>(root) {
          public void operate(Relative<R> r) {
            tap.initialize(r);
          }
        }.start();
      }
    };
  }
}
