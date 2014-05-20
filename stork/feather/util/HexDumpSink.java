package stork.feather.util;

import java.io.*;

import stork.feather.*;

/**
 * A {@code Sink} which prints a hexadecimal representation of incoming data to
 * an {@link OutputStream} or, by default, to {@code System.out}. This is used
 * for testing Feather {@code Tap}s.
 */
public class HexDumpSink extends Sink<AnonymousResource> {
  private PrintStream out;
  private boolean first = true;

  /**
   * Create a {@code HexDumpSink} that prints to {@code System.out}.
   */
  public HexDumpSink() {
    super((AnonymousResource) Resource.ANONYMOUS);
    out = System.out;
  }

  /**
   * Create a {@code HexDumpSink} that prints to {@code out}.
   *
   * @param out the {@code OutputStream} to write to.
   * @throws NullPointerException if {@code out} is {@code null}.
   */
  public HexDumpSink(OutputStream out) {
    super((AnonymousResource) Resource.ANONYMOUS);
    if (out == null)
      throw new NullPointerException();
    this.out = new PrintStream(out);
  }

  /**
   * Create a {@code HexDumpSink} that prints to {@code out}.
   *
   * @param out the {@code PrintWriter} to write to.
   * @throws NullPointerException if {@code out} is {@code null}.
   */
  public HexDumpSink(PrintStream out) {
    super((AnonymousResource) Resource.ANONYMOUS);
    if (out == null)
      throw new NullPointerException();
    this.out = out;
  }

  public Bell<AnonymousResource>
  initialize(Relative<AnonymousResource> resource) {
    if (first)
      out.println();
    first = false;
    out.println("Beginning dump: "+resource.path);
    return null;
  }

  public void drain(Relative<Slice> slice) {
    out.println(slice);
  }

  public void finalize(Relative<AnonymousResource> resource) {
    out.println("End of dump: "+resource.path);
  }
}
