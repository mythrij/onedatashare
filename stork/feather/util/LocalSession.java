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
public class LocalSession extends Session<LocalSession,LocalResource> {
  private ThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4);
  final Path path;

  /** Create a {@code LocalSession} at the system root. */
  public LocalSession() { this(Path.ROOT); }

  /** Create a {@code LocalSession} at {@code path}. */
  public LocalSession(Path path) {
    super(URI.create("file:///").path(path));
    this.path = path;
  }

  public LocalResource select(Path path) {
    return new LocalResource(this, path);
  }

  // A special runnable bell that will be scheduled with the executor, and will
  // cancel the queued task if the bell is cancelled.
  abstract class TaskBell<T> extends Bell<T> {
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

  public static void main(String[] args) {
    LocalSession s = new LocalSession();
    LocalResource r = s.select("/home/bwross/test");

    r.stat().new Then() {
      public void done(Stat stat) {
        System.out.println(stork.ad.Ad.marshal(stat));
      } public void fail(Exception e) {
        e.printStackTrace();
      }
    };
  }
}
