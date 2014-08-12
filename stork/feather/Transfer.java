package stork.feather;

import java.util.concurrent.*;

import stork.feather.util.*;

/**
 * A handle on the state of a data transfer. This class contains methods for
 * controlling data flow and monitoring data throughput. This is the base class
 * for {@code ProxyTransfer} as well as any custom {@code Transfer} controller.
 * <p/>
 * Control operations are performed on a {@code Transfer} through {@code
 * public} {@code Bell} members. This allows implementors to make assumptions
 * about what states certain control methods may be called from.
 *
 * @param <S> the source {@code Resource} type.
 * @param <D> the destination {@code Resource} type.
 */
public abstract class Transfer<S extends Resource, D extends Resource> {
  public final S source;
  public final D destination;

  private Time timer;
  private Progress progress = new Progress();
  private Throughput throughput = new Throughput();

  private boolean startCalled = false;
  private final Bell onStart = new Bell() {
    public void done() {
      System.out.println("Transfer starting...");
      if (!Transfer.this.isDone())
        timer = new Time(); 
    } public void fail(Throwable t) {
      onStop.ring(t);
    }
  };
  private final Bell onStop = new Bell() {
    public void done() {
      if (timer != null) timer.stop();
      System.out.println("Transfer complete.");
      System.out.println("Total:  "+progress);
      System.out.println("Avg.Th: "+progress.rate(timer));
    } public void fail(Throwable t) {
      if (timer != null) timer.stop();
      System.out.println("Transfer failed!");
      t.printStackTrace();
    } public void always() {
      onStart.cancel();
    }
  };

  /**
   * Create a {@code Transfer} from {@code source} to {@code destination}.
   *
   * @param source the source {@code Resource}.
   * @param destination the destination {@code Resource}.
   */
  public Transfer(S source, D destination) {
    this.source = source;
    this.destination = destination;
  }


  /** Get the source {@code Resource}. */
  public final S source() { return source; }

  /** Get the destination {@code Resource}. */
  public final D destination() { return destination; }

  /** Start the transfer. */
  public final synchronized void start() { onStart.ring(); }

  /**
   * Start this transfer when {@code bell} rings.
   *
   * @param bell a {@code Bell} whoses ringing indicates the transfer should
   * start. If {@code Bell} fails, the transfer fails with the same {@code
   * Throwable}.
   */
  public final void startOn(Bell bell) { bell.promise(onStart); }

  /** Stop the transfer. */
  public final void stop() { onStop.ring(); }

  /**
   * Fail the transfer with the given reason. Subclasses should call {@code
   * super.stop()} when the transfer has completed.
   *
   * @param reason a {@code Throwable} indicating the reason the transfer
   * failed.
   */
  public final void stop(Throwable reason) { onStop.ring(reason); }

  /**
   * Cancel the transfer. This is equivalent to failing the transfer with a
   * {@code CancellationException}.
   */
  public final void cancel() { stop(new CancellationException()); }

  /**
   * Stop this transfer when {@code bell} rings.
   *
   * @param bell a {@code Bell} whoses ringing indicates the transfer should
   * stop. If {@code Bell} fails, the transfer fails with the same {@code
   * Throwable}.
   */
  public final void stopOn(Bell bell) {
    bell.new Promise() {
      public void done() { start(); }
      public void fail(Throwable t) { stop(t); }
    };
  }

  /**
   * Pause the transfer temporarily. {@code resume()} should be called to
   * resume transfer after pausing. Implementors should assume this method will
   * only be called from a running state.
   */
  protected void pause() { }

  /**
   * Resume the transfer after a pause. Implementors should assume this method
   * will only be called from a paused state.
   */
  protected void resume() { }

  /** Check if the transfer is complete. */
  public final boolean isDone() {
    return onStop.isDone();
  }

  /** Used by subclasses to note progress. */
  protected final void addProgress(long size) {
    progress.add(size);
    throughput.update(size);
  }

  /**
   * Check if the pipeline is capable of draining {@code Slice}s in arbitrary
   * order. The return value of this method should remain constant across
   * calls.
   *
   * @return {@code true} if transmitting slices in arbitrary order is
   * supported.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public boolean random() { return false; }

  /**
   * Get the number of distinct {@code Resource}s the pipeline may be in the
   * process of transferring simultaneously.
   * <p/>
   * Returning a number less than or equal to zero indicates that an arbitrary
   * number of {@code Resource}s may be transferred concurrently.
   *
   * @return The number of data {@code Resource}s this sink can receive
   * concurrently.
   * @throws IllegalStateException if this method is called when the pipeline
   * has not been initialized.
   */
  public int concurrency() { return 1; }

  /**
   * Return a {@code Bell} which rings when the {@code Transfer} starts.
   *
   * @return A {@code Bell} which rings when the {@code Transfer} starts.
   */
  public final Bell<Transfer<S,D>> onStart() {
    return onStart.as(this);
  }

  /**
   * Return a {@code Bell} which rings when the {@code Transfer} stops.
   *
   * @return A {@code Bell} which rings when the {@code Transfer} stops.
   */
  public final Bell<Transfer<S,D>> onStop() {
    return onStop.as(this);
  }
}
