package stork.feather;

import java.util.concurrent.*;

// A session represents a connection to a remote endpoint and all associated
// configuration and state. It also represents a root resource from which
// subresources may be extracted. All session operations should be performed
// asynchronously and return immediately.

public abstract class Session extends Resource {
  // Common transfer options.
  public boolean overwrite = true;
  public boolean verify    = false;
  public boolean encrypt   = false;
  public boolean compress  = false;

  // Create a session from a URL.
  public Session(String u) {
    this(URI.create(u));
  } public Session(URI u) {
    super(u);
  }

  // Of course the session of the root resource is this session.
  public Session session() {
    return this;
  }

  // Get a directory listing of a path from the session.
  public abstract Bell<Stat> stat(String path);

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
  public final Resource select(String path) {
    return select(URI.create(path));
  } public abstract Resource select(URI uri);
}
