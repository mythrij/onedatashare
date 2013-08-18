package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import stork.scheduler.*;
import static stork.util.StorkUtil.Static.*;
import stork.stat.*;
import stork.cred.*;

import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

public class GridFTPModule extends TransferModule {
  public static final String name    = "Stork GridFTP Module";
  public static final String version = "0.2";

  public GridFTPModule() {
    super(new Ad("name", "Stork GridFTP Module")
            .put("version", "0.2")
            .put("author", "Brandon Ross")
            .put("email", "bwross@buffalo.edu")
            .put("description", "A module for FTP and GridFTP transfers.")
            .put("protocols", "gsiftp", "gridftp", "ftp")
            .put("params", "parallelism", "x509_proxy", "optimizer"));
  }

  // Interface methods
  // -----------------
  // Create a new connection to an FTP server.
  public StorkSession session(EndPoint e) {
    return new GridFTPSession(e);
  }
}
