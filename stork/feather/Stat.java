package stork.feather;

import java.util.*;

/**
 * Metadata about a resource, including subresources if the resource is a
 * directory. Many of the members of this class are determined on a best-effort
 * basis, depending on the implementation, and should not be taken to be
 * absolutely correct.
 */
public class Stat {
  /** The name of the resource. */
  public String     name;
  /** The size of the resource in bytes. */
  public long       size;
  /** The modification time of the resource in Unix time. */
  public long       time;
  /** Whether or not the resource is a directory. */
  public boolean    dir;
  /** Whether or not the resource is a file. */
  public boolean    file;
  /** An implementation-specific permissions string. */
  public String     perm;
  /** An array of subresources, if known. */
  public Stat[]     files;

  private transient long total_size = -1;
  private transient long total_num  =  0;

  /**
   * Create a new {@code Stat} with no name.
   */
  public Stat() { this(null); }

  /**
   * Create a new {@code Stat} with the given name. The name will be
   * internalized for the sake of conserving memory.
   */
  public Stat(CharSequence name) {
    if (name != null)
      this.name = Intern.string(name.toString());
  }

  /**
   * Get the total size of the tree.
   */
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

  /**
   * Copy the data from the passed file tree into this one.
   */
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

  /**
   * Get the total number of items under this tree.
   */
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

  /**
   * Return a path up to the parent.
   */
  public String path() {
    return name;
  }

  /**
   * Set the files underneath this tree and reset cached values.
   */
  public Stat setFiles(Collection<Stat> fs) {
    return setFiles(fs.toArray(new Stat[fs.size()]));
  }

  /**
   * Set the files underneath this tree and reset cached values.
   */
  public Stat setFiles(Stat[] fs) {
    files = fs;
    total_size = -1;
    total_num  =  0;

    return this;
  }
}
