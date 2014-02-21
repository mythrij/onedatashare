package stork.module.ftp;

import stork.feather.*;

/**
 * An FTP-specific {@link Resource}.
 */
public class FTPResource implements Resource {
  /**
   * Create a resource accessible from the given session.
   *
   * @param session the {@link FTPSession} the exists on
   * @param path the path to the resource relative to the session root
   */
  public FTPResource(FTPSession session, String path) {
  }
}
