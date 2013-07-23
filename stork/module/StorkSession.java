package stork.module;

import stork.ad.*;
import stork.util.*;
import stork.scheduler.*;
import java.net.URI;

// Represents a connection to a remote end-point. A session should
// provide methods for starting a transfer, listing directories, and
// performing other operations on the end-point.

public abstract class StorkSession {
  public EndPoint ep = null;

  protected Pipe<Ad>.End pipe = null;
  protected StorkSession pair = null;
  protected boolean closed = false;

  ////////////////////////////////////////////////////////////////
  // The following methods should be implemented by subclasses: //
  ////////////////////////////////////////////////////////////////

  // Get a directory listing of a path from the session.
  protected abstract Ad listImpl(String path, Ad opts);

  // Get the size of a file given by a path.
  protected abstract long sizeImpl(String path);

  // Create a directory at the end-point, as well as any parent directories.
  // Returns whether or not the command succeeded.
  //protected abstract boolean mkdirImpl(String path);

  // Transfer from this session to a paired session.
  protected abstract void transferImpl(String src, String dest, Ad opts);

  // Close the session and free any resources.
  protected abstract void closeImpl();

  // Create an identical session with the same settings. Can optionally
  // duplicate its pair as well and pair the duplicates.
  //public abstract StorkSession duplicate(boolean both);

  // Check if this session can be paired with another session. Override
  // this in subclasses.
  public boolean pairCheck(StorkSession other) {
    return true;
  }

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
  public void setPipe(Pipe<Ad>.End pipe) {
    this.pipe = pipe;
  }

  // This should only be used by transfer modules themselves to report
  // progress. TODO: Make protected once we get gridftp settled.
  //protected void reportProgress(Ad... ad) {
  public void reportProgress(Ad... ad) {
    System.out.println("Reporting: "+ad[0]);
    if (pipe != null) pipe.put(ad);
  }

  // Public interfaces to abstract methods.
  public Ad list(String path) {
    return list(path, null);
  } public Ad list(String path, Ad opts) {
    checkConnected();
    path = StorkUtil.normalizePath(path);
    if (opts == null)
      opts = new Ad();
    return listImpl(path, opts);
  }

  public long size(String path) {
    checkConnected();
    return sizeImpl(path);
  }

  public void transfer(String src, String dest, Ad opts) {
    checkConnected();
    if (pair == null)
      throw new FatalEx("session is not paired");
    if (opts == null)
      opts = new Ad();
    src = StorkUtil.normalizePath(src);
    dest = StorkUtil.normalizePath(dest);
    transferImpl(src, dest, opts);
  }

  // Check if the session hasn't been closed. Throws exception if so.
  private void checkConnected() {
    if (!isConnected())
      throw new FatalEx("session has been closed");
  }

  // Check if the session is connected or if it's closed.
  public synchronized boolean isConnected() {
    return !closed;
  } public synchronized boolean isClosed() {
    return closed;
  }

  // Close the session, cancel and transfers, free any resources.
  public synchronized void close() {
    unpair();
    closeImpl();
    closed = true;
  }

  // Pair this session with another session. Calls pairCheck() first
  // to see if the sessions are compatible. Returns paired session.
  public synchronized StorkSession pair(StorkSession other) {
    if (other != null && pair == other)
      throw new FatalEx("these sessions are already paired");
    if (other == this)
      throw new FatalEx("cannot pair a session with itself");
    if (other != null && (!pairCheck(other) || !other.pairCheck(this)))
      throw new FatalEx("other session is not compatible with this session");
    if (other != null && other.pair != null)
      throw new FatalEx("other session is already paired");
    if (other == null)
      return null;
    if (pair != null)
      pair.pair(null);
    if (other != null)
      other.pair = this;
    return pair = other;
  }

  // Unpair the session. Equivalent to calling pair(null).
  public synchronized void unpair() {
    pair(null);
  }

  // Get the paired session.
  public synchronized StorkSession pair() {
    return pair;
  }

  // Transfer a file from this session to a paired session. Throws an
  // error if the session has not been paired. opts may be null.
  public void transfer(String src, String dest) {
    transfer(src, dest, null);
  }

  // Get the protocol used by the session.
  public String protocol() {
    return ep.proto();
  }

  // Get the authority for the session.
  public String authority() {
    return ep.uri.getAuthority();
  }
}
