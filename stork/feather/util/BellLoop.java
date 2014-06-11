package stork.feather.util;

import stork.feather.*;

/**
 * An experimental {@code Bell}-driven loop.
 */
public abstract class BellLoop extends Bell {
  private final Object lock;

  /** Create a {@code BellLoop} synchronized on itself. */
  public BellLoop() { lock = this; }

  /**
   * Create a {@code BellLoop} synchronized on {@code lock}. Everytime looping
   * is done, the {@code lock} will be synchronized on.
   */
  public BellLoop(Object lock) { this.lock = lock; }

  /**
   * Start looping immediately.
   */
  public void start() { start(null); }

  /**
   * Start looping when {@code bell} rings. If {@code bell} is {@code null},
   * start immediately.
   */
  public void start(Bell bell) {
    if (bell == null)
      bell = new Bell().ring();
    bell.promise(this);
  }

  /**
   * When a {@code Bell} this {@code BellLoop} is promised to rings, start the
   * loop.
   */
  protected final void then(Object o) {
    doIterate();
  }

  private void doIterate() {
    Bell bell = null;
    synchronized (lock) {
      bell = lock();
    } if (bell == null) {
      bell = new Bell().ring();
    } if (!isDone()) try {
      bell.new Promise() {
        public void then() throws Exception {
          body();
          doIterate();
        } public void fail(Throwable t) {
          BellLoop.this.ring(t);
        }
      };
    } catch (Throwable t) {
      ring(t);
    }
  }

  /**
   * Return a {@code Bell} that will ring when the next iteration of the loop
   * should run. If {@code null} is returned, the body executes immediately.
   */
  public abstract Bell lock();

  /**
   * The iteration operation performed by the loop. This method should return a
   * {@code Bell} that will ring when the next iteration should be performed.
   * This method may stop the loop by ringing the {@code BellLoop}.
   */
  public abstract void body() throws Exception;
}
