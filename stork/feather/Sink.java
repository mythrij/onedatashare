package stork.feather;

// A sink is a destination for slices. It can be thought of as a "drain" for
// data sources.

public interface Sink {
  // This is called when an upstream tap emits a slice. If, after writing, the
  // sink can no longer accept slices, it should cork the tap the slice came
  // from, and uncork it when more data can be written.
  void write(Slice data);

  // This is called when an upstream tap encounters an error while retrieving a
  // resource. Depending on the nature of the error, the sink should take
  // action.
  void write(ResourceError error);
}
