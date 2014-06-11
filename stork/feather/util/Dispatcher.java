package stork.feather.util;

import java.util.concurrent.*;

import stork.feather.*;

/** A dispatch loop used internally. */
public final class Dispatcher {
  private static final Dispatcher instance = new Dispatcher();
  private final ExecutorService pool = Executors.newFixedThreadPool(4);

  /**
   * Schedule {@code runnable} to be executed sometime in the future.
   */
  public static void dispatch(Runnable runnable) {
    instance.pool.submit(runnable);
  }

  public static abstract class Task implements Runnable {
    { dispatch(this); }
    public abstract void run();
  }
}
