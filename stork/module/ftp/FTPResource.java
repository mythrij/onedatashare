package stork.module.ftp;

import stork.feather.*;

/**
 * An FTP-specific {@link Resource}.
 */
public class FTPResource implements Resource {
  public final FTPSession session;
  public final URI uri;

  /**
   * This should only be called by {@link FTPSession}. It will attempt to use this
   * resource as the session root.
   */
  protected FTPResource(URI uri) {
    if (this instanceof FTPSession)
      session = (FTPSession) this;
    else
      throw new Error("nullary constructor must be called by FTPSession");
    this.uri = uri;
  }

  /**
   * Create an {@code FTPResource} which belongs to the given session.
   *
   * @param session the session the resource belongs to
   * @param path the path this resource can be found at relative to the session
   * root
   */
  protected FTPResource(FTPSession session, String path) {
    this.session = session;
    //uri = session.uri().append(path);
    uri = URI.create(session.uri()+path);
  }

  public FTPSession session() {
    return session;
  }

  public URI uri() {
    return uri;
  }

  public Bell<Stat> stat() {
    return session.stat(uri.path());
  }

  public Bell<Void> mkdir() {
    return session.mkdir(uri.path());
  }

  public Bell<Void> rm() {
    return session.rm(uri.path());
  }

  /**
   * This method will detect if the named {@link Resource} supports third-party
   * transfer, and attempt to establish a third-party data channel if so. If
   * not, or if the channel cannot be established, it falls back to a general
   * proxy transfer.
   *
   * @param resource the resource to perform a transfer to
   */
  public Bell<Transfer> transferTo(Resource resource) {
    if (resource instanceof FTPResource)
      return tryThirdParty((FTPResource) resource);
    return Transfer.proxy(this, resource);
  }

  /**
   * Try to do a third-party transfer between this resource and another FTP
   * resource. If a third-party connection cannot be established, fall back to
   * a proxy transfer.
   *
   * @param resource the resource to perform a transfer to
   */
  public Bell<Transfer> tryThirdParty(FTPResource resource) {
    return Transfer.proxy(this, resource);
  }

  public Bell<Sink> sink() {
    return (Bell<Sink>) session.ch.new DataChannel() {{
      new Command("STOR", uri.path());
      unlock();
    }}.bell();
  }

  public Bell<Tap> tap() {
    return (Bell<Tap>) session.ch.new DataChannel() {{
      new Command("RETR", uri.path());
      unlock();
    }}.bell();
  }
}
