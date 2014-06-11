package stork.feather.util;

import java.io.*;

import stork.feather.*;

/**
 * A {@code Sink} which prints a hexadecimal representation of incoming data to
 * an {@link OutputStream} or, by default, to {@code System.out}. This is used
 * for testing Feather {@code Tap}s.
 */
public class HexDumpSink extends Sink<HexDumpResource> {
  private PrintStream out = System.out;
  private boolean first = true;

  /**
   * Create a {@code HexDumpSink} that prints to {@code System.out}.
   */
  public HexDumpSink() {
    this(new HexDumpSession().root());
  } private HexDumpSink(HexDumpResource r) {
    super(r);
  }

  /**
   * Create a {@code HexDumpSink} that prints to {@code out}.
   *
   * @param out the {@code OutputStream} to write to.
   * @throws NullPointerException if {@code out} is {@code null}.
   */
  public HexDumpSink(OutputStream out) {
    this();
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
    this();
    if (out == null)
      throw new NullPointerException();
    this.out = out;
  }

  public void start(Bell bell) { bell.ring(); }

  public void drain(Slice slice) {
    out.println(slice);
  }

  public void finish() {
    out.println("End of dump: "+path());
  }
}

class HexDumpResource extends Resource<HexDumpSession, HexDumpResource> {
  public HexDumpResource(HexDumpSession s, Path path) { super(s, path); }
  public HexDumpSink sink() { return new HexDumpSink(); }
}

class HexDumpSession extends Session<HexDumpSession, HexDumpResource> {
  public HexDumpSession() { super(URI.EMPTY); }
  public HexDumpResource select(Path path) {
    return new HexDumpResource(this, path);
  }
}
