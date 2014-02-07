package stork.feather;

import java.util.*;

import stork.util.*;

// Metadata about a resource, including subresources if the resource is a
// directory.

public class Stat {
  public String     name;
  public long       size;
  public long       time;
  public boolean    dir;
  public boolean    file;
  public String     perm;
  public Stat[]     files;

  private transient long total_size = -1;
  private transient long total_num  =  0;

  public Stat() {
    this(null);
  } public Stat(CharSequence s) {
    if (s != null)
      name = Intern.string(s.toString());
  }

  // Get the total size of the tree.
  public long size() {
    if (total_size >= 0) {
      return total_size;
    } if (dir) {
      long s = size;
      if (files != null) for (Stat f : files)
        s += f.dir ? 0 : f.size();
      return total_size = s;
    } return total_size = size;
  }

  // Copy the data from the passed file tree into this one.
  public Stat copy(Stat ft) {
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
      if (files != null) for (Stat f : files)
        n += f.count();
      return total_num = n;
    } return total_num = 1;
  }

  // Return a path up to the parent.
  public String path() {
    return name;
  }

  // Set the files underneath this tree and reset cached values.
  public Stat setFiles(Collection<Stat> fs) {
    return setFiles(fs.toArray(new Stat[fs.size()]));
  } public Stat setFiles(Stat[] fs) {
    files = fs;
    total_size = -1;
    total_num  =  0;

    return this;
  }
}
