package stork.feather.util;

import java.io.*;
import java.util.concurrent.*;

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
public class LocalSession extends Session {
  private ThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(10);

  /**
   * Create a {@code LocalSession} with the system root as the session root.
   */
  public LocalSession() {
    this(Path.ROOT);
  }

  /**
   * Create a {@code LocalSession} with {@code path} as the session root.
   */
  public LocalSession(Path path) {
    super(new URIBuilder().scheme("file").path(path));
  }

  // A special runnable bell that will be scheduled with the executor, and will
  // cancel the queued task if the bell is cancelled.
  private abstract class TaskBell<T> extends Bell<T> {
    private final Future future = executor.submit(new Runnable() {
      public final void run() {
        try {
          task();
          ring();
        } catch (Exception e) {
          ring(e);
        }
      }
    });

    // Override this to perform some task and optionally ring the bell. If the
    // bell is not rung at the end of the task, it will be rung with null. Any
    // exception thrown will fail the bell.
    public abstract void task() throws Exception;

    public final void fail(Throwable t) {
      if (t instanceof CancellationException)
        future.cancel(false);
    }
  }

  public Bell doMkdir(Resource resource) {
    final File file = new File(resource.uri().path().toString());
    return new TaskBell() {
      public void task() {
        if (file.exists() && file.isDirectory())
          throw new RuntimeException("Resource is a file.");
        else if (!file.mkdirs())
          throw new RuntimeException("Could not create directory.");
      }
    };
  }

  public Bell doRm(Resource resource) {
    final File root = new File(resource.uri().path().toString());
    return new TaskBell() {
      public void task() {
        remove(root);
      }

      // Recursively remove files.
      private void remove(File file) {
        if (isCancelled()) {
          throw new CancellationException();
        } if (!file.exists()) {
          if (file == root)
            throw new RuntimeException("Resource does not exist.");
        } else {
          // If not a symlink and is a directory, delete contents.
          if (file.getCanonicalFile().equals(file) && file.isDirectory())
            for (File f : file.listFiles()) delete(f);
          if (!file.delete() && file == root)
            throw new RuntimeException("Resource could not be deleted.");
        }
      }
    };
  }

  public Bell doStat(Resource resource) {
    final File file = new File(resource.uri().path().toString());
    return new TaskBell<Stat>() {
      public void task() {
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

  public static void main(String[] args) {
    new LocalSession().stat("/home/bwross").promise(new Bell<Stat>() {
      public void done(Stat stat) {
        System.out.println(stork.ad.Ad.marshal(stat));
      } public void fail(Exception e) {
        e.printStackTrace();
      }
    });
  }
}
