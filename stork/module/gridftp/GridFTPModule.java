package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.scheduler.*;

public class GridFTPModule extends TransferModule {
  public GridFTPModule() {
    super("Stork GridFTP Module", "gsiftp", "gridftp", "ftp");
    version = "0.2";
    author  = "Brandon Ross";
    email   = "bwross@buffalo.edu";
    description = "A module for FTP and GridFTP transfers.";
    options(GridFTPOptions.class);
  }

  // Interface methods
  // -----------------
  // Create a new connection to an FTP server.
  public StorkSession session(EndPoint e) {
    return new GridFTPSession(e);
  }
}
