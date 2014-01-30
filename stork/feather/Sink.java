package stork.feather;

// A sink is a destination for slices. It can be thought of as a "drain" for
// data sources.

public interface Sink {
  // This is called when an upstream tap emits a slice.
  void write(Slice data);

  // This is called when an upstream tap emits an exception.
  void write(ResourceError error);
}
