package stork.feather;

import java.util.concurrent.*;
import java.net.URI;

import stork.scheduler.*;

// Represents a connection to a remote endpoint and all associated
// configuration and state. A session should provide methods for starting a
// transfer, listing directories, and performing other operations on the
// endpoint asynchronously.

public abstract class Session {
  public transient Endpoint ep = null;

  // Common transfer options.
  public boolean overwrite = true;
  public boolean verify    = false;
  public boolean encrypt   = false;
  public boolean compress  = false;

  // Create a session from a URL. Generally the path is ignored.
  public Session(String u) {
    this(new Endpoint(u));
  } public Session(URI u) {
    this(new Endpoint(u));
  } public Session(Endpoint e) {
    ep = e;
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

  // Return a resource object representing the referent of the given URI.
  public final Resource select(String path) {
    return select(URI.create(path));
  } public abstract Resource select(URI uri);

  // Get the protocol used by the session.
  public final String protocol() {
    return ep.proto();
  }

  // Get the authority for the session.
  public final String authority() {
    return ep.uri()[0].getAuthority();
  }
}
