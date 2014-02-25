package stork.feather.util;

import java.io.*;
import stork.feather.*;

/**
 * A "connection" to the local file system, used for testing Feather
 * implementations.
 */
/*
public abstract class LocalSession extends LocalResource {
  private final File root;

  public LocalSession(String u) {
    super(URI.create("file:"+u));
    root = new File(u);
  }

  public abstract Bell<Stat> stat(String path) {
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
  public abstract Bell<Void> close();

  // Create an identical session with the same settings.
  //public abstract Session duplicate();

  // Return a tap to the resource returned by selecting the root URI.
  public abstract Bell<Tap> tap();

  // Return a sink to the resource returned by selecting the root URI.
  public abstract Bell<Sink> sink() {
    return select(uri).sink();
  }
}
*/
