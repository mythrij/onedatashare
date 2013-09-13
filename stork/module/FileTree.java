package stork.module;

import stork.util.*;
import stork.ad.*;
import java.util.*;

// This represents a file listing tree to be used by transfer modules. It
// can safely be marshalled as an ad and used to serialize and recover the
// state of a transfer in progress.
// TODO: Progress tracking.

public class FileTree {
  public transient FileTree parent;

  public String     name;
  public long       size;
  public long       time;
  public boolean    dir;
  public boolean    file;
  public String     perm;
  public FileTree[] files;

  private transient long total_size = -1;
  private transient long total_num  =  0;

  public FileTree() {
    this(null);
  } public FileTree(CharSequence s) {
    if (s != null)
      name = Intern.string(s.toString());
  }

  // Get the total size of the tree.
  public long size() {
    if (total_size >= 0) {
      return total_size;
    } if (dir) {
      long s = size;
      if (files != null) for (FileTree f : files)
        s += f.dir ? 0 : f.size();
      return total_size = s;
    } return total_size = size;
  }

  // Copy the data from the passed file tree into this one.
  public FileTree copy(FileTree ft) {
    if (name == null)
      name = ft.name;
    size = ft.size;
    time = ft.time;
    dir  = ft.dir;
    file = ft.file;
    perm = ft.perm;
    return this;
  }

  // Get the total number of items under this tree.
  public long count() {
    if (total_num > 0) {
      return total_num;
    } if (dir) {
      long n = 1;
      if (files != null) for (FileTree f : files)
        n += f.count();
      return total_num = n;
    } return total_num = 1;
  }

  // Return a path up to the parent.
  public String path() {
    if (parent == null)
      return name;
    return parent.path()+"/"+name;
  }

  // Set the files underneath this tree and reset cached values.
  public FileTree setFiles(Collection<FileTree> fs) {
    return setFiles(fs.toArray(new FileTree[fs.size()]));
  } public FileTree setFiles(FileTree[] fs) {
    files = fs;
    total_size = -1;
    total_num  =  0;

    if (fs != null) for (FileTree t : fs)
      t.parent = this;

    return this;
  }
}
