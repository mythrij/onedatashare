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
  private boolean mlsx;
  private Reader reader;
  private StringBuilder sb;
  private boolean closed = false;

  // Pass an ad to write into and whether or not this is an mlsx listing.
  public ListAdSink(Ad ad, boolean mlsx) {
    this.ad = ad;
    this.mlsx = mlsx;
    sb = new StringBuilder();
  }

  // Write a byte array of MLSD output to the sink.
  public synchronized void write(byte[] buf) {
    sb.append(new String(buf));
  }

  // Write a JGlobus buffer to the sink.
  public synchronized void write(Buffer buffer) throws IOException {
    System.out.println("Got buffer: "+new String(buffer.getBuffer()));
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

  // Pattern to match whole lines.
  private static Pattern line_pattern = Pattern.compile("(?m)^.+$");

  // Parse all of the buffered listing contents and store in ad.
  public void finalizeList() {
    String line;
    Matcher m = line_pattern.matcher(sb);

    // Read lines from the buffer list.
    if (mlsx)
      parseMlsx(m);
    else
      parseList(m);
  }

  // Parse MLSX entries into ad.
  private void parseMlsx(Matcher m) {
    while (m.find()) {
      String line = m.group();
      try {
        MlsxEntry me = new MlsxEntry(line);
        //System.out.println("MLSX Line: "+line);

        String name = me.getFileName();
        String type = me.get("type");
        long size = Long.parseLong(me.get("size"));

        if (type.equals(me.TYPE_FILE))
          addFile(new Ad("name", name).put("size", size));
        else if (!name.equals(".") && !name.equals(".."))
          addDir(new Ad("name", name));
      } catch (Exception e) {
        e.printStackTrace();
        continue;  // Weird data I guess!
      }
    }
  }

  // Parse Unix listing into ad.
  private void parseList(Matcher m) {
    while (m.find()) {
      String line = m.group();
      try {
        FileInfo fi = new FileInfo(line);
        //System.out.println("LIST Line: "+line);

        String name = fi.getName();
        boolean dir = fi.isDirectory();
        long size = fi.getSize();

        if (name.equals(".") || name.equals(".."))
          continue;
        if (!dir)
          addFile(new Ad("name", name).put("size", size));
        else 
          addDir(new Ad("name", name));
      } catch (Exception e) {
        e.printStackTrace();
        continue;  // Weird data I guess!
      }
    }
  }

  // Add a file to the tree.
  void addFile(Ad file) {
    try {
      files = files.next(file);
    } catch (Exception e) {
      ad.put("files", files = file);
    }
  }

  // Add a dir to the tree.
  void addDir(Ad dir) {
    try {
      dirs = dirs.next(dir);
    } catch (Exception e) {
      ad.put("dirs", dirs = dir);
    }
  }
}
