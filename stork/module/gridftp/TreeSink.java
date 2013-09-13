package stork.module.gridftp;

import stork.module.*;
import stork.util.*;
import java.io.*;
import org.globus.ftp.*;

// JGlobus DataSink implementation that feeds into FTP list parser.

class TreeSink implements DataSink {
  private Bell<FileTree> tb;
  private FTPListParser parser;

  // Pass a bell to ring when close is called. Also accepts a listing
  // format, if one is known.
  public TreeSink(Bell<FileTree> tb) {
    this(tb, null, 0);
  } public TreeSink(Bell<FileTree> tb, FileTree base, int t) {
    this.tb = tb;
    parser = new FTPListParser(base, t);
  }

  // Write a byte array to the sink.
  public synchronized void write(byte[] buf) {
    parser.write(buf);
  } public synchronized void write(Buffer buffer) throws IOException {
    //System.out.println("Got buffer: "+new String(buffer.getBuffer()));
    parser.write(buffer.getBuffer());
  }

  public synchronized void close() throws IOException {
    tb.ring(parser.finish());
  }
}
