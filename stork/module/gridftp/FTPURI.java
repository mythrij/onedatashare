package stork.module.gridftp;

import stork.util.*;
import static stork.module.ModuleException.*;
import stork.cred.*;
import stork.scheduler.*;

import java.net.*;
import java.util.*;
import java.io.*;

// Wraps a URI and a credential into one object and makes sure the URI
// represents a supported protocol. Also parses out a bunch of stuff.
// TODO: Remove this.

public class FTPURI {
  public URI uri;
  public StorkGSSCred cred = null;

  public boolean gridftp, ftp, file;
  public String host, proto;
  public int port;
  public String user, pass;
  public String path;

  private void setUserPass(String[] ui) {
    if (ui == null)
      return;
    if (ui[0] != null)
      user = ui[0];
    if (ui[1] != null)
      pass = ui[1];
  }

  public FTPURI(URI uri, StorkCred cred) {
    this.uri = uri;

    host = uri.getHost();
    proto = uri.getScheme();
    int p = uri.getPort();
    setUserPass(StorkUserinfo.split(uri));

    // Only use the credential if we support it.
    if (cred == null) {
      this.cred = null;
    } else if (cred instanceof StorkUserinfo) {
      setUserPass(((StorkUserinfo) cred).credential());
    } else if (cred instanceof StorkGSSCred) {
      this.cred = (StorkGSSCred) cred;
    } else throw abort("unsupported credential type: "+cred.type());

    if (uri.getPath().startsWith("/~"))
      path = uri.getPath().substring(1);
    else
      path = uri.getPath();

    // Check protocol and determine port.
    if (proto == null || proto.isEmpty()) {
      throw abort("no protocol specified");
    } if ("gridftp".equals(proto) || "gsiftp".equals(proto)) {
      port = (p > 0) ? p : 2811;
      gridftp = true; ftp = false; file = false;
    } else if ("ftp".equals(proto)) {
      port = (p > 0) ? p : 21;
      gridftp = false; ftp = true; file = false;
    } else if ("file".equals(proto)) {
      port = -1;
      gridftp = false; ftp = false; file = true;
    } else {
      throw abort("unsupported protocol: "+proto);
    }
  }
}
