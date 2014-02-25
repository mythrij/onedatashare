package stork.feather;

/**
 * A handle on the state of a data transfer.
 */

public abstract class Transfer {
  public static Bell<Transfer> proxy(Resource src, Resource dest) {
    return proxy(src.tap(), dest.sink());
  }
  public static Bell<Transfer> proxy(Tap tap, Sink sink) {
    return proxy(new Bell<Tap>().ring(tap), new Bell<Sink>().ring(sink));
  }
  public static Bell<Transfer> proxy(Tap tap, Bell<Sink> sink) {
    return proxy(new Bell<Tap>().ring(tap), sink);
  }
  public static Bell<Transfer> proxy(Bell<Tap> tap, Sink sink) {
    return proxy(tap, new Bell<Sink>().ring(sink));
  }
  public static Bell<Transfer> proxy(
      final Bell<Tap> tap, final Bell<Sink> sink) {
    final Bell<Transfer> bell = new Bell<Transfer>();
    new Bell.All(tap, sink) {
      public void done() {
        tap.sync().attach(sink.sync());
        bell.ring(new Transfer() { });  // TODO
      } public void fail(Throwable t) {
        bell.ring(t);
      }
    };
    return bell;
  }
}
