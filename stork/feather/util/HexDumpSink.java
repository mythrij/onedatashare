package stork.feather.util;

import java.io.*;

import stork.feather.*;

/**
 * A {@code Sink} which prints a hexadecimal representation of incoming data to
 * an {@link OutputStream} or, by default, to {@link #System.out}. This is used
 * for testing Feather {@code Tap}s.
 */
public class HexDumpSink extends Sink {
  private PrintStream out;
  private boolean first = true;

  /**
   * Create a {@code HexDumpSink} that prints to {@link #System.out}.
   */
  public HexDumpSink() {
    out = System.out;
  }

  /**
   * Create a {@code HexDumpSink} that prints to {@code out}.
   *
   * @param out the {@code OutputStream} to write to.
   * @throws NullPointerException if {@code out} is {@code null}.
   */
  public HexDumpSink(OutputStream out) {
    if (out == null)
      throw new NullPointerException();
    this.out = new PrintWriter(out);
  }

  /**
   * Create a {@code HexDumpSink} that prints to {@code out}.
   *
   * @param out the {@code PrintWriter} to write to.
   * @throws NullPointerException if {@code out} is {@code null}.
   */
  public HexDumpSink(PrintWriter out) {
    if (out == null)
      throw new NullPointerException();
    this.out = out;
  }

  public Bell<?> initialize(Relative<Resource> resource) {
    if (first)
      out.println();
    first = false;
    out.println("Beginning dump: "+resource.path);
    return null;
  }

  public void drain(Relative<Slice> slice) {
    out.println(slice);
  }

  public void finalize(Relative<Resource> resource) {
    out.println("End of dump: "+resource.path);
  }
}
