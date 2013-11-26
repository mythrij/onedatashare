package stork.module.sftp;

import stork.ad.*;
import stork.util.*;
import stork.module.*;
import stork.scheduler.*;
import java.net.URI;

public class SFTPModule extends TransferModule {
  public SFTPModule() {
    super("Stork SFTP/SCP Module", "scp", "sftp");
    version = "0.1";
    author  = "Brandon Ross";
    email   = "bwross@buffalo.edu";
    description =
      "A module for SFTP/SCP transfers.";
  }

  // Create a new connection to an FTP server.
  public StorkSession session(EndPoint e) {
    return new SFTPSession(e);
  }
}
