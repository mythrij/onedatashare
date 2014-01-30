package stork.feather;

// A tap emits data slices. It should not begin emitting data until a sink has
// been attached.

public interface Tap {
  // This is called by client code to attach a source to a sink.
  void attach(Sink sink);
}
