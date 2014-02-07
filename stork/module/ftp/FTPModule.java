package stork.module.ftp;

import stork.ad.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;

public class FTPModule extends TransferModule {
  public FTPModule() {
    super("Stork FTP Module", "ftp", "gsiftp", "gridftp");
    version = "0.1";
    author  = "Brandon Ross";
    email   = "bwross@buffalo.edu";
    description =
      "A module for interacting with FTP systems and derivatives thereof. "+
      "Supports RFC 2228 security extensions with GSSAPI, as well as a "+
      "few GridFTP extensions.";
  }

  // Interface methods
  // -----------------
  // Create a new connection to an FTP server.
  public Session session(Endpoint e) {
    return FTPSession.connect(e).sync();
  }
}
