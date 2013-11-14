package stork.module;

import stork.ad.*;
import stork.util.*;
import stork.scheduler.*;
import static stork.module.ModuleException.*;
import java.net.URI;

// Represents a connection to a remote end point. A session should
// provide methods for starting a transfer, listing directories, and
// performing other operations on the end point.

public abstract class StorkSession {
  public transient EndPoint ep = null;

  protected transient Pipe<Ad>.End pipe = null;
  protected transient boolean closed = false;

  // Common transfer options.
  public boolean overwrite = true;
  public boolean verify    = false;
  public boolean encrypt   = false;
  public boolean compress  = false;

  ////////////////////////////////////////////////////////////////
  // The following methods should be implemented by subclasses: //
  ////////////////////////////////////////////////////////////////

  // Get a directory listing of a path from the session.
  protected abstract Bell<FileTree> listImpl(String path, Ad opts);

  // Get the size of a file given by a path.
  protected abstract Bell<Long> sizeImpl(String path);

  // Create a directory at the end-point, as well as any parent directories.
  protected abstract Bell<?> mkdirImpl(String path);

  // Remove a file or directory.
  protected abstract Bell<?> rmImpl(String path);

  // Close the session and free any resources.
  protected abstract void closeImpl();

  // Create an identical session with the same settings.
  //public abstract StorkSession duplicate();

  // Get a channel to a session resource.
  protected abstract StorkChannel openImpl(String base, FileTree ft);

  ////////////////////////////////////////////////////////////////////
  // Everything below this point should be more or less left alone. //
  ////////////////////////////////////////////////////////////////////

  // Create a session from a URL. Generally the path is ignored.
  public StorkSession(String u) {
    this(new EndPoint(u));
  } public StorkSession(URI u, Ad opts) {
    this(new EndPoint(u));
  } public StorkSession(EndPoint e) {
    ep = e;
  }

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

  // Public interfaces to abstract methods.
  public final Bell<FileTree> list(String path) {
    return list(path, null);
  } public final Bell<FileTree> list(String path, Ad opts) {
    checkConnected();
    path = StorkUtil.normalizePath(path);
    if (opts == null)
      opts = new Ad();
    return listImpl(path, opts);
  }

  public final long size(String path) {
    checkConnected();
    path = StorkUtil.normalizePath(path);
    return sizeImpl(path).waitFor();
  }

  public final void mkdir(String path) {
    checkConnected();
    path = StorkUtil.normalizePath(path);
    mkdirImpl(path).waitFor();
  }

  public final void rm(String path) {
    checkConnected();
    path = StorkUtil.normalizePath(path);
    rmImpl(path).waitFor();
  }

  // Check if the session hasn't been closed. Throws exception if so.
  private final void checkConnected() {
    if (!isConnected())
      throw abort("session has been closed");
  }

  public final StorkChannel open(String path) {
    String d = StorkUtil.dirname(path), b = StorkUtil.basename(path);
    return open(d, new FileTree(b));
  } public final StorkChannel open(String base, FileTree ft) {
    return openImpl(base, ft);
  }

  // Check if the session is connected or if it's closed.
  public final synchronized boolean isConnected() {
    return !closed;
  } public final synchronized boolean isClosed() {
    return closed;
  }

  // Close the session, cancel and transfers, free any resources.
  public final synchronized void close() {
    closeImpl();
    closed = true;
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
