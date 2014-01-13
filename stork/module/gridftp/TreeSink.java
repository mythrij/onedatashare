package stork.module.gridftp;

import stork.module.*;
import stork.util.*;
import java.io.*;
import org.globus.ftp.*;

// JGlobus DataSink implementation that feeds into FTP list parser. This
// can be attached to a GridFTP data channel as a sink, and will ring an
// attached bell with the parsed file tree when the listing is complete.

class TreeSink extends Bell.Single<FileTree> implements DataSink {
  private FTPListParser parser;

  // Pass a bell to ring when close is called. Also accepts a listing
  // format, if one is known.
  public TreeSink() {
    this(null, 0);
  } public TreeSink(FileTree base, int t) {
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
    ring(parser.finish());
  }
}
