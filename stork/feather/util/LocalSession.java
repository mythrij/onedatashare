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
  final ThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(4);
  final Path path;

  /** Create a {@code LocalSession} at the system root. */
  public LocalSession() { this(Path.ROOT); }

  /** Create a {@code LocalSession} at {@code path}. */
  public LocalSession(Path path) {
    super(URI.create("file:"+path));
    this.path = path;
  }

  public LocalResource select(Path path) {
    return new LocalResource(this, path);
  }

  protected void finalize() {
    executor.shutdown();
  }

  public static void main(String[] args) {
    String sp = args.length > 0 ? args[0] : "/home/bwross/test";
    final Path path = Path.create(sp);
    final LocalSession s = new LocalSession(path);

    s.root().tap().attach(new HexDumpSink()).onStop().new Promise() {
      public void done() { s.close(); }
    };
  }
}
