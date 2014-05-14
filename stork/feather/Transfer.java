package stork.feather;

enum State { UNSTARTED, STARTING, RUNNING, PAUSED, STOPPED }

/**
 * A handle on the state of a data transfer. This class contains methods for
 * controlling data flow and monitoring data throughput. This is the base class
 * for {@code ProxyTransfer} as well as any custom {@code Transfer} controller.
 * <p/>
 * Control operations must be performed on a {@code Transfer} through its
 * {@code mediator} member. This allows implementors to make assumptions about
 * what states certain control methods may be called from by imposing access
 * control mechanisms dependant on the state of the {@code Transfer}.
 *
 * @param <S> the source {@code Resource} type.
 * @param <D> the destination {@code Resource} type.
 */
public abstract class Transfer<S extends Resource, D extends Resource> {
  private State state = State.UNSTARTED;

  // A bell which will ring when the transfer starts.
  private final Bell<Transfer<S,D>> onStart = new Bell<Transfer<S,D>>() {
    protected void done() {
      state = State.RUNNING;
    } protected void fail(Throwable t) {
      onStop.ring(Transfer.this);
    }
  };

  // A bell which will ring when the transfer stops.
  private final Bell<Transfer<S,D>> onStop = new Bell<Transfer<S,D>>() {
    protected void always() {
      state = State.STOPPED;
    }
  };

  // If we get paused, a bell will be placed here to resume the transfer.
  private Bell<Transfer<S,D>> pauseBell;

  /** Mediator for controlling this {@code Transfer}. */
  public final Mediator mediator = new Mediator();

  /** State imposition and access mediator for {@code Transfer}s. */
  public final class Mediator {
    private Bell<Transfer<S,D>> pendingPause;

    private Mediator() { }

    /** Start the transfer. The returned {@code Bell} rings on start. */
    public synchronized Bell<Transfer<S,D>> start() {
      if (state == State.UNSTARTED) try {
        state = State.STARTING;
        Bell<?> bell = Transfer.this.start();
        if (bell != null)
          bell.thenAs(Transfer.this).promise(onStart);
        else
          onStart.ring(Transfer.this);
      } catch (Exception e) {
        onStart.ring(e);
      } return onStart;
    }

    /** Stop the transfer. Cancel any pending bells. */
    public synchronized void stop() {
      if (state != State.STOPPED) try {
        Transfer.this.stop();
      } catch (Exception e) {
        // What should we do with this...?
      } finally {
        onStart.cancel();  // Will also cancel pendingPause.
        if (paused()) pauseBell.cancel();
        onStop.ring(Transfer.this);
      }
    }

    /** Pause the transfer. The returned {@code Bell} rings on resume. */
    public synchronized Bell<Transfer<S,D>> pause() {
      switch (state) {
        case PAUSED:  // Paused, just return existing bell.
          return pauseBell.new Promise();
        case UNSTARTED:
        case STARTING:  // Call back later.
          if (pendingPause == null) pendingPause = onStart.new Then() {
            public void then(Transfer<S,D> transfer) {
              pause().promise(this);  // Note, this is the mediator pause.
            } public void always() {
              pendingPause = null;
            }
          };
          return pendingPause;
        case RUNNING:  // Tell the transfer to pause.
          try {
            Transfer.this.pause();
          } catch (Exception e) {
            return new Bell<Transfer<S,D>>().ring(e);
          }

          // Don't leak the actual bell.
          return (pauseBell = new Bell<Transfer<S,D>>() {
            { state = State.PAUSED; }
            public void done()   { Transfer.this.resume(); }
            public void always() { pauseBell = null; }
          }).new Promise();
        default:  // Nothing to do...
          return new Bell<Transfer<S,D>>().cancel();
      }
    }

    /** Resume the transfer. */
    public synchronized void resume() {
      if (paused())
        pauseBell.ring();
    }
  }

  /**
   * Get the source {@code Resource}.
   */
  public abstract S source();

  /**
   * Get the destination {@code Resource}.
   */
  public abstract D destination();

  /**
   * Called when this {@code Transfer} is started through the mediator. This
   * method may return a {@code Bell} which will ring when the {@code Transfer}
   * is ready, or {@code null} to indicate that it is ready immediately.
   * <p/>
   * Exceptions thrown here or through the returned {@code Bell} will cause the
   * {@code Transfer} to be stopped.
   * <p/>
   * No other transfer methods will be called until this method has completed
   * and the returned {@code Bell}, if non-{@code null}, has rung. This method
   * will be called at most once.
   *
   * @return A {@code Bell} which will ring when the {@code Transfer} has
   * started, or {@code null} if it is ready immediately.
   */
  protected Bell<?> start() throws Exception { return null; }

  /**
   * This is called when the {@code Transfer} has been terminated. Once
   * stopped, the transfer cannot be started again. This may be called before
   * {@link #start()} if the transfer is canceled before it starts.
   */
  protected void stop() { }

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

  /**
   * Check if this {@code Transfer} has not been started nor stopped.
   *
   * @return {@code true} if the transfer is unstarted; {@code false}
   * otherwise.
   */
  public final boolean unstarted() {
    synchronized (mediator) { return state == State.UNSTARTED; }
  }

  /**
   * Check if this {@code Transfer} is in the starting state.
   *
   * @return {@code true} if the transfer is starting; {@code false} otherwise.
   */
  public final boolean starting() {
    synchronized (mediator) { return state == State.STARTING; }
  }

  /**
   * Check if the {@code Transfer} is running; that is, it has started and is
   * neither stopped nor paused. A running {@code Transfer} may be paused.
   *
   * @return {@code true} if the transfer is running.
   */
  public final boolean running() {
    synchronized (mediator) { return state == State.RUNNING; }
  }

  /**
   * Check if the {@code Transfer} has stopped.
   *
   * @return {@code true} if the transfer has stopped.
   */
  public final boolean stopped() {
    synchronized (mediator) { return state == State.STOPPED; }
  }

  /**
   * Check if the {@code Transfer} is paused, but not stopped. Another way of
   * looking at it is to check if the {@code Transfer} can be resumed.
   *
   * @return {@code true} if the transfer is paused.
   */
  public final boolean paused() {
    synchronized (mediator) { return state == State.PAUSED; }
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
}
