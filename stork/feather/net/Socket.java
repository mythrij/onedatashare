package stork.feather.net;

import stork.feather.*;

/**
 * An asynchrounous network connection.
 */
public abstract class Socket extends Coder<byte[],byte[]> {
  /** This will hold the result of doOpen(). */
  private Bell<?> doOpen;

  /** Ring this to start the opening process. */
  private final Bell<Void> startOpening = new Bell<Void>() {
    public void done() {
      try {
        doOpen = doOpen();
        doOpen.as((Void) null).promise(onOpen);
      } catch (Exception e) {
        fail(e);
      }
    } public void fail(Throwable t) {
      onOpen.ring(t);
    }
  };

  /** Will ring after the connection has been established. */
  private final Bell<Void> onOpen = new Bell<Void>() {
    public void fail(Throwable t) { doClose.ring(t); }
  };

  /** Ring this to start the closing process. */
  private final Bell<Void> doClose = new Bell<Void>() {
    public void always() {
      if (doOpen != null)
        doOpen.cancel();
      startOpening.cancel();
      try {
        doClose();
      } catch (Exception e) {
        // Ignore.
      }
    }
  };

  /** Start the connection process. */
  public final synchronized Socket open() {
    startOpening.ring();
    return this;
  }

  /** Start the connection process when {@code bell} rings. */
  public final Socket openOn(Bell<?> bell) {
    bell.as((Void) null).promise(startOpening);
    return this;
  }

  /** Return a bell that rings when the socket is opened. */
  public final Bell<Socket> onOpen() { return onOpen.as(this); }

  /** Check if the channel is opened. */
  public final boolean isOpened() { return onOpen.isDone(); }

  /** Close the socket. */
  public final Socket close() {
    doClose.ring();
    return this;
  }

  /** Close the socket for the given reason. */
  public final Socket close(Throwable reason) {
    doClose.ring(reason);
    return this;
  }

  /** Close the socket when {@code bell} rings. */
  public final Socket closeOn(Bell<?> bell) {
    bell.as((Void) null).promise(doClose);
    return this;
  }

  /** Return a bell that rings when the socket is opened. */
  public final Bell<Socket> onClose() { return doClose.as(this); }

  /** Check if the channel is closed. */
  public final boolean isClosed() { return doClose.isDone(); }

  /**
   * Start a connection timeout timer for this channel. The returned bell can
   * be cancelled to stop the timeout timer.
   */
  public final Bell<?> timeout(double time) {
    Bell<?> bell = Bell.timerBell(time);
    bell.new Promise() {
      public void done() {
        if (!isOpened()) close();
      }
    };
    return bell;
  }

  /**
   * Handle asynchronously opening this socket's channel. This will be called
   * at most once.
   *
   * @return A bell which should ring when the connection has been established
   * or has failed to be established. If cancelled, the connection process
   * should be halted.
   * @throws Exception Any exception thrown will be ignored.
   */
  protected abstract Bell<?> doOpen() throws Exception;

  /**
   * Handle closing this socket's channel. This will be called at most once.
   *
   * @throws Exception Any exception thrown will be ignored.
   */
  protected abstract void doClose() throws Exception;
}
