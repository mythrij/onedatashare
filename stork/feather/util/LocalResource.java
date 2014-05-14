package stork.feather.util;

import java.io.*;

import stork.feather.*;

/**
 * A session capable of interacting with the local file system. This is
 * intended to serve as an example Feather implementation, but can also be used
 * for testing other implementations.
 * <p/>
 * Many of the methods in this session implementation perform long-running
 * operations concurrently using threads because this is the most
 * straighforward way to demonstrate the asynchronous nature of session
 * operations. However, this is often not the most efficient way to perform
 * operations concurrently, and ideal implementations would use an alternative
 * method.
 */
public class LocalResource extends Resource<LocalSession,LocalResource> {
  public LocalResource(LocalSession session, Path path) {
    super(session, path);
  }

  // Get the absolute path to the file.
  private Path path() {
    return path.absolutize(session.path);
  }

  // Get the File based on the path.
  private File file() {
    return new File(path().toString());
  }

  public Bell mkdir() {
    return session.new TaskBell() {
      public void task() {
        File file = file();
        if (file.exists() && file.isDirectory())
          throw new RuntimeException("Resource is a file.");
        else if (!file.mkdirs())
          throw new RuntimeException("Could not create directory.");
      }
    };
  }

  public Bell unlink() {
    return session.new TaskBell() {
      private File root = file();

      public void task() {
        remove(root);
      }

      // Recursively remove files.
      private void remove(File file) {
        if (isCancelled()) {
          throw new java.util.concurrent.CancellationException();
        } if (!file.exists()) {
          if (file == root)
            throw new RuntimeException("Resource does not exist.");
        } else try {
          // If not a symlink and is a directory, delete contents.
          if (file.getCanonicalFile().equals(file) && file.isDirectory())
            for (File f : file.listFiles()) remove(f);
          if (!file.delete() && file == root)
            throw new RuntimeException("Resource could not be deleted.");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public Bell stat() {
    return session.new TaskBell<Stat>() {
      public void task() {
        File file = file();

        if (!file.exists())
          throw new RuntimeException("Resource does not exist.");

        Stat stat = new Stat(file.getName());
        stat.size = file.length();
        stat.setFiles(file.list());
        stat.time = file.lastModified();

        ring(stat);
      }
    };
  }
}
