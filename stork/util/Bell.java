package stork.util;

import java.util.*;
import java.util.concurrent.*;

// Utility classes and interfaces for bells.

public final class Bell {
  // Bell states
  public static enum State {
    UNRUNG, SUCCESS, FAILURE;

    // Fluency methods for ring().
    public boolean succeeded() {
      return this == SUCCESS;
    } public boolean failed() {
      return this != SUCCESS;
    } public boolean rang() {
      return this != UNRUNG;
    }
  }

  // Interfaces
  // ==========
  // Bells that can be rung with objects of type I. The ring methods return a
  // boolean indicating whether or not the bell was rung, and thus now contains
  // a value. The "promise" side of the bell.
  public interface In<I> {
    // Ring the bell with nothing, setting the value to null. Returns whether
    // or not the value caused the bell to ring.
    State ring();

    // Ring the bell with an object. Returns whether or not the value caused
    // the bell to ring.
    State ring(I i);

    // Ring the bell with an exception, causing it to fail. Returns whether or
    // not the value caused the bell to ring.
    State ring(Throwable t);
  }

  // Bells that yield objects of type O when read. The "future" side of the
  // bell. This extends the Java future interface to support promise
  // pipelining with then().
  public interface Out<O> extends Future<O> {
    // Ring the given bells when this bell rings.
    void then(In<? super O> in);
    void then(Collection<In<? super O>> in);
    O get();
  }

  // A bell that is rung with objects of type I and yields objects of type O.
  public interface To<I,O> extends In<I>, Out<O> { }

  // Base Implementations
  // ====================
  // A simple base class for implementing handlers for then(). Multiple
  // variants of the handler methods are exposed, allowing the opportunity for
  // less verbose method signatures in subclasses. Well-behaved subclasses will
  // implement at most one variant of each of done, fail, and always. The
  // method with the most specific signature will be the one ultimately called.
  //
  // Any of the handler methods may throw to fail the handler. Note that in
  // Java, the "throws" part is optional when overriding methods, unless the
  // method actually throws an exception that must be handled.
  public static abstract class Handler<I> implements In<I> {
    public State ring()            { return ring(null, null); }
    public State ring(I i)         { return ring(i, null); }
    public State ring(Throwable t) { return ring(null, t); }
    private State ring(I i, Throwable t) {
      try {
        callHandlers(i, t);
        return State.SUCCESS;
      } catch (Throwable th) {
        return State.FAILURE;
      }
    }

    // This basic implementation simply calls the handlers, which are expected
    // to be overridden by subclasses. Subclasses can further override this to
    // add additional handler semantics.
    protected void callHandlers(I i, Throwable t) throws Throwable {
      try {
        if (t != null)
          fail(t);
        else
          done(i);
      } catch (Throwable th) {
        t = th;
      } finally {
        always(i, t);
      } if (t != null) {
        throw t;
      }
    }

    // Called when the bell is rung successfully. This is analogous to the "try"
    // part of a try-catch-finally construction.
    public void done() throws Throwable {
      // Override this if you don't care about the ringing value.
    } public void done(I i) throws Throwable {
      // Override this to check the value. This is the done handler that will
      // actually get called on ring. By default, it delegates to nullary
      // method done().
      done();
    }

    // Called when the bell is rung with an exception. This is analogous to the
    // "catch" part of a try-catch-finally construction.
    public void fail() throws Throwable {
      // Override this if you don't care about the exception.
    } public void fail(Throwable t) throws Throwable {
      // Override this to see the value and change it by returning a new value.
      // This is the handler that will actually get called on failure. By default,
      // it delegates to nullary method fail().
      fail();
    }

    // This will be called at the end of handling (i.e., after done() or fail()
    // has executed), regardless of whether the bell failed or succeeded. It
    // may change the value by returning or throwing. This is analogous to the
    // "finally" part of a try-catch-finally construction.
    public void always() throws Throwable {
      // Override this if you do not care about the ringing value. By default,
      // this does nothing.
    } public void always(I i) throws Throwable {
      // Override this to only see the value. By default, this does nothing.
      always();
    } public void always(Throwable t) throws Throwable {
      // Override this to only see the throwable. By default, this does nothing.
      always();
    } public void always(I i, Throwable t) throws Throwable {
      // Override this to see either the ringing value or the error. This is the
      // actual method called when the handlers are called.
      if (t == null)
        always(i);
      else
        always(t);
    }
  }

  // A base class for bells that exhibit typical "out" behavior.
  public static abstract class BasicOut<I,O> extends Handler<I>
  implements To<I,O> {
    State state = State.UNRUNG;
    private O value;
    private Throwable error;
    private Set<In<? super O>> thens;

    // Get the stored value. If the bell has not rung, this method blocks until
    // it rings. If the bell has failed, this method throws the exception
    // wrapped in an ExecutionException.
    public synchronized O get() {
      while (!isDone()) try {
        wait();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } return getValue();
    }

    // Get the stored value. If the bell has not rung, this method blocks until
    // it rings, up to a given amount of time. If the bell has failed, this
    // method throws the exception wrapped in an ExecutionException.
    public synchronized O get(long timeout, TimeUnit unit)
    throws TimeoutException {
      if (isDone()) {
        return getValue();
      } try {
        unit.timedWait(this, timeout);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } if (isDone()) {
        return getValue();
      } throw new TimeoutException();
    }

    // Helper method which either returns the value or throws the wrapped
    // exception. This should ONLY be called when the bell is known to be
    // resolved.
    protected synchronized O getValue() {
      switch (state) {
        case SUCCESS:
          return value;
        case FAILURE :
          if (error == null)
            throw new CancellationException();
          throw new RuntimeException(error);
        default:
          throw new Error("bell has not rung");
      }
    }

    // Cancelling simply resolves the bell with a cancellation exception.
    public synchronized boolean cancel(boolean may_interrupt) {
      if (state != State.UNRUNG)
        return false;
      resolve((Throwable) null);
      return true;
    }

    // Check if the bell has been resolved.
    public synchronized boolean isDone() {
      return state != State.UNRUNG;
    }

    // Check if the bell resolved successfully.
    public synchronized boolean isSuccessful() {
      return state == State.SUCCESS;
    }

    // Check if the bell resolved successfully.
    public synchronized boolean isFailed() {
      return state == State.FAILURE;
    }

    // Check if the bell has been cancelled.
    public synchronized boolean isCancelled() {
      return state == State.FAILURE && error == null;
    }

    // This should be used by subclasses to resolve the bell.
    synchronized void resolve(O value) {
      this.value = value;
      state = State.SUCCESS;
      notifyAll();
      ringOthers(thens);
    } synchronized void resolve(Throwable error) {
      this.error = error;
      state = State.FAILURE;
      notifyAll();
      ringOthers(thens);
    }

    // Schedule a compatible bell to be rung after this bell rings. If this
    // bell has already rung when then() is called, the passed bell will be
    // rung immediately with this bell's value.
    public synchronized void then(In<? super O> bell) {
      // Java generics syntax suuuucks.
      In<O> bell2 = (In<O>) bell;
      then((In<? super O>) Collections.singleton(bell2));
    } public synchronized void then(Collection<In<? super O>> bells) {
      if (isDone()) {
        ringOthers(bells);
      } else {
        if (thens == null)
          thens = new HashSet<In<? super O>>();
        thens.addAll(bells);
      }
    }

    // Helper method for ringing thenned bells. ONLY call this if this bell has
    // been rung.
    private synchronized void ringOthers(Collection<In<? super O>> bells) {
      if (bells != null) try {
        O v = getValue();
        for (In<? super O> b : bells) if (b != this) try {
          b.ring(v);
        } catch (Exception e) {
          // This is here to absorb any runtime exceptions that might come out
          // of ring() and pass it off to the thenned bell. This can happen,
          // e.g., someone is playing fast and loose with our generic variadic
          // parameters. Bad code monkey, bad!
          b.ring(e);
        }
      } catch (Throwable t) {
        // At this point, ringing with a throwable should not throw any
        // exceptions.
        for (In<? super O> b : bells) if (b != this) b.ring(t);
      }
    }
  }

  // This special exception is thrown by ring handlers to ignore ringing
  // values.
  private static class DiscardedRingException extends RuntimeException { }

  // This special exception is thrown by ring handlers to alter the final
  // value of this bell.
  private static class AlterationException extends RuntimeException {
    final BasicTo.Holder holder;
    AlterationException(BasicTo.Holder h, Throwable t) {
      super(t);
      holder = h;
    }
  }

  // A base class for all bells that exhibit typical behavior. This class
  // provides on-ring handlers which can be used to perform tasks when the bell
  // is rung, and potentially alter or ignore values.
  //
  // A few special methods can be called inside the ring handlers to control the ring
  // result:
  //
  // alter(...)
  //   Changes the final value of the bell. Calling alter() will not result
  //   in the handlers being called multiple times, though calling alter() in
  //   done() or fail() will cause always() to see the changed value. Calling
  //   alter() will also bypass the call to convert().
  //              
  // discard()
  //   Ignores the ringing value and leaves the bell unrung. The handlers
  //   will be called again when the bell is rung in the future.
  public static abstract class BasicTo<I,O> extends BasicOut<I,O> {
    // This is a workaround to Java's infuriating restrictions regarding
    // throwables as inner classes and generic casting. Because we can't
    // directly cast the value held by an AlterationException to an O, we have
    // to use this stupid indirection technique, and cast the "holder" held by
    // the exception instead. The compiler complains in the name of "type
    // safety", but at least it lets it happen. I don't know why the compiler
    // is so concerned about type safety in this case when there are already so
    // many ways within the language to break type safety anyway. Maybe it
    // should mind its own business, feh.
    class Holder { O value; }

    // Subclasses need to define the method for converting input objects into
    // output values.
    protected abstract O convert(I i);

    // Try to ring the bell and return the bell's state.
    public final synchronized State ring(I i) {
      return ring(i, null);
    } public final synchronized State ring(Throwable t) {
      if (t == null)
        t = new NullPointerException();
      return ring(null, t);
    } public final synchronized State ring() {
      return ring(null, null);
    } protected synchronized State ring(I i, Throwable t) {
      // At least one must be null.
      if (i != null && t != null)
        throw new Error("rung with both an object and an exception");
      // Subclasses should handle this check themselves if they want different
      // behavior.
      if (state != State.UNRUNG)
        throw new Error("bell has already rung");

      // The calling of the handlers has been moved into a separate method for
      // resuability purposes. This will handle calling resolve() as well.
      callHandlers(i, t);

      return state;
    }

    // This may be called by either done, fail, or always to ignore the ringing
    // value and leave the bell unrung.
    protected final void discard() {
      throw new DiscardedRingException();
    }

    // This may be called by either done, fail, or always to alter the final
    // result of the ring. Calling this throws a special runtime exception. As
    // such, to work, the exception thrown by this method should not be
    // discarded by a handler.
    protected final void alter(Throwable t) {
      alter(null, t);
    } protected final void alter(O o) {
      alter(o , null);
    } protected final void alter() {
      alter(null, null);
    } private final void alter(O o, Throwable t) {
      // Annoying workaround. See remarks above Holder class definition.
      Holder h = new Holder();
      h.value = o;
      throw new AlterationException(h, t);
    }

    // This method will call the handlers and take care of whatever havoc they
    // may wreak (i.e., throwing special control exceptions).
    protected void callHandlers(I i, Throwable t) {
      callHandlers(i, null, t, false);
    } private void callHandlers(I i, O o, Throwable t, boolean always) {
      // Try running the handlers and intercept any exceptions they throw.
      try {
        if (!always) {
          // First call the done() and fail() handlers. These may alter what
          // get passed to always() in the second iteration.
          if (t != null) {
            fail(t);
          } else {
            done(i);
            o = convert(i);
          }
        } else {
          // Now call the always() handler, which may see something different
          // than done() and fail() saw if alter() was called.
          always(i, t);
        }
      } catch (DiscardedRingException e) {
        // The handlers chose to ignore the ring.
        return;
      } catch (AlterationException ae) {
        // alter() was called, so update the resolution values we're working
        // with.
        o = ((Holder) ae.holder).value;
        t = ae.getCause();
      } catch (Throwable th) {
        // The handler threw some other exception. Update the state with that.
        t = th;
      }

      // What we do next depends on whether we called always() or not...
      if (!always) {
        // We need to call this again, but run always() instead.
        callHandlers(i, o, t, true);
      } else {
        // We just ran the always() handler. Whatever values we have now are
        // the values we will use to resolve the bell.
        if (t != null) resolve(t);
        else           resolve(o);
      }
    }
  }

  // A base class for all single-type bells.
  public static abstract class Basic<T> extends BasicTo<T,T> {
    public Basic()            { }
    public Basic(T t)         { ring(t); }
    public Basic(Throwable t) { ring(t); }

    // These simply use the input as the output.
    public final T convert(T t) { return t; }
  }

  // A bell that, once rung, cannot be rung again (future rings will be
  // ignored). Calls to get() will block if the bell has not rung.
  public static class Single<I> extends Basic<I> {
    public Single()            { super(); }
    public Single(I i)         { super(i); }
    public Single(Throwable t) { super(t); }

    protected synchronized State ring(I i, Throwable t) {
      try {
        return super.ring(i, t);
      } catch (Throwable th) {
        // This will only happen if the bell was already rung. Single
        // assignment bells just ignore it.
      } return state;
    }
  }

  // A base class for a bell which can be used to gather and act on the values
  // of its children as they ring. The reduction behavior can be defined by
  // subclasses by implementing reduce(). Subclasses may ring inside of either
  // implementation of reduce(). If the bell has not rung when all of the
  // sub-bells have rung and been reduced, this bell will automatically be rung
  // with the result of defaultValue().
  public static abstract class Reduce<I,O> extends Single<O> {
    private Set<Out<? extends I>> set;
    protected O defaultValue = null;
    protected Throwable defaultError = null;

    protected Reduce(Out<? extends I>... bells) {
      this(Arrays.asList(bells));
    } protected Reduce(Collection<Out<? extends I>> bells) {
      set = new HashSet<Out<? extends I>>(bells);
      if (bells.isEmpty()) {
        ring();
      } else for (final Out<? extends I> b : bells) b.then(
        new Handler<I>() {
          public void always() { doReduce(b); }
        }
      );
    }

    // Wrap the reduction step, and make sure the bell gets set once the set is
    // empty.
    private synchronized final void doReduce(Out<? extends I> b) {
      if (set == null || set.isEmpty()) {
        return;
      } else if (set.remove(b)) try {
        reduce(b.get());
      } catch (Throwable t) {
        reduce(t);
      } if (set.isEmpty() && !isDone()) try {
        ring(defaultValue());
      } catch (Throwable t) {
        ring(t);
      } else if (isDone()) {
        set.clear();
        set = null;
      }
    }

    // Subclasses implement the reduction logic. Throwing in either of these
    // will ring this bell with the exception.
    protected <T extends I> void reduce(T v) { }
    protected void reduce(Throwable t) { }

    // The value to set the bell to if reduce() does not ring it. This can be
    // overridden, or the variables defaultValue or defaultError can be set by
    // subclasses.
    protected O defaultValue() throws Throwable {
      if (defaultError != null)
        throw defaultError;
      return defaultValue;
    }
  }

  // A bell which will ring only when all the target bells have rung (in which
  // case it will ring successfully with the value of the last ringing bell),
  // or one of the bells fails (in which case it will ring with the exception
  // the failed bell rung with).
  public static class All extends Reduce<Object,Object> {
    public All(Out<? extends Object>... bells)          { super(bells); }
    public All(Collection<Out<? extends Object>> bells) { super(bells); }

    protected void reduce(Object o) {
      defaultValue = o;
    } protected void reduce(Throwable t) {
      ring(t);
    }
  }

  // A bell which will ring when any of the target bells have rung (in which
  // case it will ring successfully with the value of the bell that rung), or
  // all of the bells fails (in which case it will ring with the exception of
  // the last failed bell).
  public static class Any extends Reduce<Object,Object> {
    public Any(Out<? extends Object>... bells)          { super(bells); }
    public Any(Collection<Out<? extends Object>> bells) { super(bells); }

    protected void reduce(Object o) {
      ring(o);
    } protected void reduce(Throwable t) {
      defaultError = t;
    }
  }

  // A bell which will be rung only when all the target bells have rung. The
  // bell will contain true if all the bells succeed, false otherwise.
  public static class And extends Reduce<Object,Boolean> {
    public And(Out<? extends Object>... bells)          { super(bells); }
    public And(Collection<Out<? extends Object>> bells) { super(bells); }

    { defaultValue = Boolean.TRUE; }

    protected void reduce(Throwable t) {
      ring(Boolean.FALSE);
    }
  }

  // A bell which will be rung when at least one target bell has succeeded, or
  // all target bells have failed. The bell will contain true if at least one
  // target bell succeeded, or false otherwise.
  public static class Or extends Reduce<Object,Boolean> {
    public Or(Out<? extends Object>... bells)          { super(bells); }
    public Or(Collection<Out<? extends Object>> bells) { super(bells); }

    { defaultValue = Boolean.FALSE; }

    protected <T extends Object> void reduce(T b) {
      ring(Boolean.TRUE);
    }
  }

  // Utility Methods
  // ===============
  // Convenience method for creating an "all bell".
  public static All all(Out<? extends Object>... bells) {
    return all(Arrays.asList(bells));
  } public static All all(Collection<Out<? extends Object>> bells) {
    return new All(bells);
  }

  // Convenience method for creating an "any bell".
  public static Any any(Out<? extends Object>... bells) {
    return any(Arrays.asList(bells));
  } public static Any any(Collection<Out<? extends Object>> bells) {
    return new Any(bells);
  }

  // Convenience method for creating an "and bell".
  public static And and(Out<? extends Object>... bells) {
    return and(Arrays.asList(bells));
  } public static And and(Collection<Out<? extends Object>> bells) {
    return new And(bells);
  }

  // Convenience method for creating an "or bell".
  public static Or or(Out<? extends Object>... bells) {
    return or(Arrays.asList(bells));
  } public static Or or(Collection<Out<? extends Object>> bells) {
    return new Or(bells);
  }

  private Bell() { /* utility class, don't instantiate */ }

  public static void main(String[] args) throws Exception {
    Single<String> a = new Single<String>();
    Single<String> b = new Single<String>() {
      public void done(String o) {
        System.out.println("rang: "+o);
        alter("goodbye");
      }
    };
    a.then(b);
    a.ring("hello");
    System.out.println(a.get());
    System.out.println(b.get());
  }
}
