package stork.util;

import java.util.*;

// A list of files and directories to transfer.
// TODO: Bug fixes!

public class XferList implements Iterable<XferList.Entry> {
  private LinkedList<Entry> list = new LinkedList<Entry>();
  private long size = 0;
  private int count = 0;  // Number of files (not dirs)
  public String sp, dp;
  public final Entry root;

  // Create an XferList for a directory.
  public XferList(String src, String dest) {
    if (!src.endsWith("/"))  src  += "/";
    if (!dest.endsWith("/")) dest += "/";
    sp = src;
    dp = dest;
    root = new Entry("");
  }

  // Create an XferList for a file.
  public XferList(String src, String dest, long size) {
    sp = StorkUtil.dirname(src);
    dp = StorkUtil.dirname(dest);
    add(StorkUtil.basename(src), size);
    root = new Entry("");
  }

  // An entry (file or directory) in the list.
  public class Entry {
    private XferList parent = XferList.this;
    public String path;
    public final boolean dir;
    public boolean done = false;
    public final long size;
    public long off = 0, len = -1;  // Start/stop offsets.

    // Create a directory entry.
    Entry(String path) {
      if (path == null)
        path = "";
      else if (!path.endsWith("/"))
        path += "/";
      path = path.replaceFirst("^/+", "");
      this.path = path;
      size = off = 0;
      dir = true;
    }

    // Create a file entry.
    Entry(String path, long size) {
      this.path = path;
      this.size = (size < 0) ? 0 : size;
      off = 0;
      dir = false;
    }

    // Create an entry based off another entry with a new path.
    Entry(String path, Entry e) {
      this.path = path;
      dir = e.dir; done = e.done;
      size = e.size; off = e.off; len = e.len;
    }

    // Split off a portion from the beginning of this entry and
    // return it. Adjust this entry accordingly.
    public Entry split(long max) {
      Entry e = new Entry(path, this);
      if (dir) return e;
      e.setLength(max);
      if (e.end()) {
        off = size; len = 0; done = true;
      } else {
        e.setOffset(e.remaining());
      } return e;
    }
      
    public long setOffset(long o) {
      return off = (o < 0) ?  0 : (size < 0 || o < size) ? o : size;
    }

    public long setLength(long l) {
      return len = (l < 0) ? -1 : (size < 0 || l < size) ? l : -1;
    }

    // Returns -1 if the file's size is unknown or infinite.
    public long remaining() {
      long ml = size-off;
      return (size < 0) ? -1 :
             (len  < 0) ? ((off > size) ? 0 : size-off) :
             (len > ml) ? ml : len;
    }

    // Returns whether or not tranferring this entry would cause the rest
    // of the file to be transferred.
    public boolean end() {
      return (len < 0) || (size >= 0 && len >= size-off);
    }

    public String path() {
      return parent.sp + path;
    }

    public String dpath() {
      return parent.dp + path;
    }

    public String toString() {
      return (dir ? "Directory: " : "File: ")+path()+" -> "+dpath()+" "+
             "off="+off+", len="+len+", size="+size;
    }
  }

  // Add a directory to the list.
  public synchronized void add(String path) {
    add(new Entry(path));
  }

  // Add a file to the list.
  public synchronized void add(String path, long size) {
    add(new Entry(path, size));
  }

  // Add an entry to the list.
  public synchronized void add(Entry e) {
    e.parent = this;
    list.add(e);
    recalculate(e, true);
  }

  // Add another XferList's entries under this XferList.
  public synchronized void addAll(XferList ol) {
    addSize(ol.size);
    count += ol.count;
    for (Entry e : ol.list)
      e.parent = this;
    list.addAll(ol.list);
  }

  public synchronized long size() {
    return size;
  } 

  public synchronized int count() {
    return count;
  }

  public synchronized boolean isEmpty() {
    return list.isEmpty();
  }

  // Remove and return the topmost entry. Optionally a length can be given,
  // and, if the topmost element is larger than the length, it is split.
  public synchronized Entry pop() {
    return pop(-1);
  } public synchronized Entry pop(long max) {
    if (list.isEmpty())
      return null;
    Entry e = list.peek();
    long l = e.remaining();
    if (max < 0 || max >= l) {
      // Take whole entry.
      if (l < 0) recalculate();
      else       recalculate(e, false);
      return list.pop();
    } else {
      e = e.split(max);
      recalculate(e, false);
      return e;
    }
  }

  // Alter the size of this list, treating -1 as infinity/unknown.
  private long addSize(long s) {
    return size = (size < 0 || s < 0) ? -1 : size+s;
  } private long subSize(long s) {
    return size = (size < 0) ? size : (s < 0) ? -1 : size-s;
  }

  // Force recalculation of the list's size and count by pretending to
  // add everything again.
  private synchronized void recalculate() {
    size = 0; count = 0;
    for (Entry e : list) recalculate(e, true);
  }

  // Recalculate size/count based on addition/removal of entry. Returns
  // true if a full recalculation might determine the size of the list.
  private synchronized boolean recalculate(Entry e, boolean add) {
    if (add) {
      addSize(e.remaining());
      if (!e.dir) count++;
      return false;
    } else {
      long r = e.remaining();
      subSize(r);
      if (!e.dir && !e.end()) count--;
      return r < 0;
    }
  }

  // Get the progress of the list in terms of bytes.
  public synchronized Progress byteProgress() {
    Progress p = new Progress();

    for (Entry e : list)
      p.add(e.remaining(), e.size);
    return p;
  } 

  // Get the progress of the list in terms of files.
  public synchronized Progress fileProgress() {
    Progress p = new Progress();

    for (Entry e : list)
      p.add(e.done?1:0, 1);
    return p;
  }

  // "Steal" len bytes from another list. Directories will be taken as
  // references, with the originals still in the old list. Files fully
  // stolen will be removed from the other list. Files on the boundary
  // will be split with whatever part wasn't stolen in the other list.
  public synchronized XferList steal(XferList ol, long len) {
    boolean recal = false;  // Whether we need to fully recalc list.
    Iterator<Entry> iter = ol.iterator();

    System.out.println("steal("+len+")");

    if (len == -1 || (ol.size > 0 && len >= ol.size)) {
      // Take the whole list.
      addAll(ol); ol.list.clear();
      ol.size = 0; ol.count = 0;
    } else while (len > 0 && iter.hasNext()) {
      Entry e = iter.next();
      long es = e.remaining();

      if (e.done) {
        iter.remove();
        continue;
      } else if (e.dir) {
        // Don't do anything special.
      } else if (es < 0 || es > len) {
        // Take a part of the entry.
        e = e.split(len);
      } else {
        // Take the whole entry.
        iter.remove();
      }

      add(e);

      if (!recal)
        recal = ol.recalculate(e, false);
    }

    // Recalculate other list's length if it's worthwhile to do so.
    if (recal) ol.recalculate();

    return this;
  }

  // Split off a sublist from the front of this list which is
  // of a certain byte length.
  public synchronized XferList split(long len) {
    XferList nl = new XferList(sp, dp);
    return nl.steal(this, len);
  }

  public synchronized Iterator<Entry> iterator() {
    return list.iterator();
  }
}
