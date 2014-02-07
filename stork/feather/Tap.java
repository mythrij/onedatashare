package stork.feather;

// A tap emits data slices to an attached sink.

public interface Tap {
  // Attach a sink to the tap. The tap should not begin reading from the
  // upstream channel until a sink is attached.
  void attach(Sink s);

  // Cork the tap, preventing slices from being emitted until uncork is called.
  // While corked, the tap should avoid reading from the upstream channel
  // unless it buffers the data. Calling cork() on a corked tap does nothing.
  void cork();

  // Uncork the tap, resuming emission of slices and reading from the upstream
  // channel. Calling uncork() on an uncorked tap does nothing.
  void uncork();
}
