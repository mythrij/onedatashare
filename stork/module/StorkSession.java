package stork.module;

import java.util.concurrent.*;
import java.net.URI;

import stork.ad.*;
import stork.util.*;
import stork.scheduler.*;
import static stork.module.ModuleException.*;

// Represents a connection to a remote end point. A session should
// provide methods for starting a transfer, listing directories, and
// performing other operations on the end point.

public abstract class StorkSession implements AutoCloseable {
  public transient EndPoint ep = null;

  protected transient Pipe<Ad>.End pipe = null;

  // Common transfer options.
  public boolean overwrite = true;
  public boolean verify    = false;
  public boolean encrypt   = false;
  public boolean compress  = false;

  // Create a session from a URL. Generally the path is ignored.
  public StorkSession(String u) {
    this(new EndPoint(u));
  } public StorkSession(URI u) {
    this(new EndPoint(u));
  } public StorkSession(EndPoint e) {
    ep = e;
  }

  // Get a directory listing of a path from the session.
  public abstract Bell<FileTree> list(String path);

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
  //public abstract StorkSession duplicate();

  // Get a channel to a session resource.
  public abstract StorkChannel open(String base, FileTree ft);

  // Set an ad sink to write update ads into.
  // TODO: This is a temporary hack, remove me.
  public final void setPipe(Pipe<Ad>.End pipe) {
    this.pipe = pipe;
  }

  // This should only be used by transfer modules themselves to report
  // progress.
  public final void reportProgress(Ad... ad) {
    if (pipe != null) pipe.put(ad);
  }

  public final StorkChannel open(String path) {
    String d = StorkUtil.dirname(path), b = StorkUtil.basename(path);
    return open(d, new FileTree(b));
  }

  // Get the protocol used by the session.
  public final String protocol() {
    return ep.proto();
  }

  // Get the authority for the session.
  public final String authority() {
    return ep.uri()[0].getAuthority();
  }
}
