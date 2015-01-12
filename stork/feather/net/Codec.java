package stork.feather.net;

import stork.feather.*;

public abstract class Codec<A,B> {
  /** Name used for debugging purposes. */
  private final String name;
  /** Either this, or another codec if proxying. */
  private final Codec<A,?> proxy;
  /** The next codec in the pipeline. */
  private Codec<B,?> next;
  /** Rung when next is set. */
  private Bell<?> onJoin;
  /** Bell used to indicate pause status. */
  private Bell<?> pause;

  /** Create a {@code Codec} with no name. */
  public Codec() { this((String) null); }

  /** Create a {@code Codec} with a name (for debugging). */
  public Codec(String name) {
    this.name = name;
    proxy = this;
    onJoin = new Bell();
    pause = Bell.rungBell();
  }

  // This allows this codec to act as a proxy.
  Codec(Codec<A,?> proxy) {
    this.name = null;
    this.proxy = proxy.proxy;
  }

  /** Feed a message into the codec. */
  public final Bell<?> feed(final A a) {
    if (proxy != this) {
      return proxy.feed(a);
    } synchronized (this) {
      safeHandle(a, null);
      return pause;
    }
  }

  /** Feed an error into the codec. */
  public final Bell<?> feed(final Throwable error) {
    if (proxy != this) {
      return proxy.feed(error);
    } synchronized (this) {
      safeHandle(null, error);
      return pause;
    }
  }

  /** Use this instead of calling handle(...) directly. */
  private synchronized void safeHandle(A a, Throwable err) {
    if (err == null) try {
      handle(a);
    } catch (Throwable t) {
      err = t;
    } if (err != null) try {
      handle(err);
    } catch (Throwable t) {
      emit(t);
    }
  }

  /** Called by implementation to emit a message. */
  protected final synchronized Bell<?> emit(final B b) {
    if (next == null)
      pause(onJoin);
    return pause.new Promise() {
      public void done() { pause(next.feed(b)); }
    }.as(null);
  }

  /** Called to emit an error. */
  protected final synchronized Bell<?> emit(final Throwable error) {
    if (next == null)
      pause(onJoin);
    return pause.new Promise() {
      public void done() { pause(next.feed(error)); }
    }.as(null);
  }

  /**
   * Pause messages from this codec until {@code bell} rings. Once this is
   * called, messages will not emitted to the next codec until the bell rings.
   */
  public final void pause(Bell<?> bell) {
    if (proxy != this) {
      proxy.pause(bell);
    } else synchronized (this) {
      pause = pause.and(bell);
    }
  }

  /**
   * Handle a message fed into the codec. Once this has been called, the
   * handling of this message is the responsibility of the codec. If an
   * exception is thrown here, {@link #handle(Throwable)} will be called.
   */
  protected abstract void handle(A a) throws Throwable;

  /**
   * Handle an error fed into the codec. If this throws, the thrown error is
   * propagated downward.
   */
  protected void handle(Throwable error) throws Throwable {
    throw error;
  }

  /** Join this codec with another codec. */
  public final synchronized <C> Codec<A,C> join(Codec<B,C> codec) {
    if (next != null)
      throw new RuntimeException("Already joined");
    return reallyJoin(codec);
  }

   // This is like this so JoinedCodec can change its behavior while join(...)
   // can remain final.
  <C> Codec<A,C> reallyJoin(final Codec<B,C> bc) {
    final Codec<A,B> ab = this;
    ab.next = bc.proxy;
    onJoin.ring();
    return new JoinedCodec<A,C>(ab, bc);
  }

  /** The name of the codec, if it has one. */
  public String toString() {
    if (proxy != this)
      return proxy.toString();
    return (name != null) ? name : getClass().getSimpleName();
  }
}

final class JoinedCodec<A,B> extends Codec<A,B> {
  final Codec<A,?> ax;
  final Codec<?,B> xb;
  JoinedCodec(Codec<A,?> ax, Codec<?,B> xb) {
    super(ax);
    this.ax = ax;
    this.xb = xb;
  } <C> Codec<A,C> reallyJoin(Codec<B,C> bc) {
    xb.join(bc);
    return new JoinedCodec<A,C>(ax, bc);
  } protected final void handle(Object o) {
    throw new Error("This should never be called.");
  }
}
