package stork.feather.util;

import java.io.*;
import stork.feather.*;

/**
 * A "connection" to the local file system, used for testing Feather
 * implementations.
 */
public abstract class LocalSession extends Session {
  private final File root;

  /** Create a local session with a root relative to the given path. */
  public LocalSession(String u) {
    super(URI.create("file:"+u));
    root = new File(u);
  }

  /** Get a directory listing of a path from the session. */
  public Bell<Stat> stat(String path) {
    File f = new File(root, path);
    return null;
  }

  // Create a directory at the end-point, as well as any parent directories.
  public abstract Bell<Void> mkdir(String path);

  // Remove a file or directory.
  public abstract Bell<Void> rm(String path);

  // Close the session and free any resources. This should return immediately,
  // disallowing any further interaction, and begin the closing procedure
  // asynchronously. The cleanup should try to happen as quickly and quietly as
  // possible.
  public abstract void close();

  // Create an identical session with the same settings.
  //public abstract Session duplicate();

  // Return a tap to the resource returned by selecting the root URI.
  public Tap tap() {
    return select(uri).tap();
  }

  // Return a sink to the resource returned by selecting the root URI.
  public Sink sink() {
    return select(uri).sink();
  }

  // Return a resource object representing the referent of the given URI.
  public abstract Resource select(URI uri);
}
