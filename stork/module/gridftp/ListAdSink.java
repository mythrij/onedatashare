package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import static stork.util.StorkUtil.Static.*;
import stork.stat.*;
import stork.cred.*;

import java.net.*;
import java.util.*;
import java.io.*;
import java.util.regex.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;

// Read MLSD data into an ad.
class ListAdSink implements DataSink {
  private Ad ad, files = null, dirs = null;
  private Reader reader;
  private StringBuilder sb;
  private boolean closed = false;
  private FTPListParser parser;

  // Pass an ad to write into and whether or not this is an mlsx listing.
  public ListAdSink(Ad ad, boolean mlsx) {
    this.ad = ad;
    sb = new StringBuilder();
    parser = new FTPListParser(mlsx ? 'M' : 0);
  }

  // Write a byte array of MLSD output to the sink.
  public synchronized void write(byte[] buf) {
    sb.append(new String(buf));
  }

  // Write a JGlobus buffer to the sink.
  public synchronized void write(Buffer buffer) throws IOException {
    //System.out.println("Got buffer: "+new String(buffer.getBuffer()));
    write(buffer.getBuffer());
  }

  // Wait for the sink to be closed.
  public synchronized void waitFor() {
    while (!closed) try {
      wait();
    } catch (Exception e) {
      // Do nothing.
    }
  }

  public synchronized void close() throws IOException {
    if (!closed) {
      finalizeList();
      closed = true;
    } notifyAll();
  }

  // Parse all of the buffered listing contents and store in ad.
  public void finalizeList() {
    ad.put("files", parser.parse(sb.toString()));
  }
}
