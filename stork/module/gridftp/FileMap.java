package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import static stork.util.StorkUtil.Static.*;
import stork.cred.*;

import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

// A combined sink/source for file I/O.

@Deprecated
class FileMap implements DataSink, DataSource {
  RandomAccessFile file;
  long rem, total, base;
  
  public FileMap(String path, long off, long len) throws IOException {
    file = new RandomAccessFile(path, "rw");
    base = off;
    if (off > 0) file.seek(off);
    if (len+off >= file.length()) len = -1;
    total = rem = len;
  } public FileMap(String path, long off) throws IOException {
    this(path, off, -1);
  } public FileMap(String path) throws IOException {
    this(path, 0, -1);
  }

  public void write(Buffer buffer) throws IOException {
    //System.out.println("Got buffer: "+new String(buffer.getBuffer()));
    if (buffer.getOffset() >= 0)
      file.seek(buffer.getOffset());
    file.write(buffer.getBuffer());
  }

  public Buffer read() throws IOException {
    if (rem == 0) return null;
    int len = (rem > 0x3FFF || rem < 0) ? 0x3FFF : (int) rem;
    byte[] b = new byte[len];
    long off = file.getFilePointer() - base;
    len = file.read(b);
    if (len < 0) return null;
    if (rem > 0) rem -= len;
    return new Buffer(b, len, off);
  }
    
  public void close() throws IOException {
    file.close();
  }

  public long totalSize() throws IOException {
    return (total < 0) ? file.length() : total;
  }
}
