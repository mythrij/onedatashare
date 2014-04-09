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
  private boolean first;

  /**
   * Create a {@code HexDumpSink} that prints to {@link #System.out}.
   */
  public HexDumpSink() {
    this(System.out);
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
    this(new PrintWriter(out));
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

  protected Bell<?> initialize(RelativeResource resource) {
    if (first)
      System.out.println();
    first = false;
    System.out.println("Beginning dump: "+resource.path());
    return null;
  }

  protected void drain(RelativeSlice slice) {
    out.println(slice);
  }

  protected Bell<?> finalize(RelativeResource resource) {
    System.out.println("End of dump: "+resource.path());
    return null;
  }
}
