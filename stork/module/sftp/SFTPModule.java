package stork.module.sftp;

import stork.ad.*;
import stork.util.*;
import stork.module.*;
import stork.scheduler.*;
import java.net.URI;

public class SFTPModule extends TransferModule {
  public SFTPModule() {
    super(new Ad("name", "Stork SFTP/SCP Module")
            .put("version", "0.1")
            .put("author", "Brandon Ross")
            .put("email", "bwross@buffalo.edu")
            .put("description", "A module for SFTP/SCP transfers.")
            .put("protocols", "scp", "sftp"));
  }

  // Create a new connection to an FTP server.
  public StorkSession session(EndPoint e) {
    return null;
  }
}
