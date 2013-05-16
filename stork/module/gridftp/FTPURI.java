package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import static stork.util.StorkUtil.Static.*;
import stork.stat.*;
import stork.cred.*;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
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

// Wraps a URI and a credential into one object and makes sure the URI
// represents a supported protocol. Also parses out a bunch of stuff.
public class FTPURI {
  public final URI uri;
  public final StorkGSSCred cred;

  public final boolean gridftp, ftp, file;
  public final String host, proto;
  public final int port;
  public final String user, pass;
  public final String path;

  public FTPURI(URI uri, StorkCred cred) {
    this.uri = uri;
    host = uri.getHost();
    proto = uri.getScheme();
    int p = uri.getPort();
    String[] ui = StorkUserinfo.split(uri);
    user = ui[0];
    pass = ui[1];

    // Only use the credential if we support it.
    if (cred == null)
      this.cred = null;
    else if (cred instanceof StorkGSSCred)
      this.cred = (StorkGSSCred) cred;
    else throw new FatalEx("unsupported credential type: "+cred.type());

    if (uri.getPath().startsWith("/~"))
      path = uri.getPath().substring(1);
    else
      path = uri.getPath();

    // Check protocol and determine port.
    if (proto == null || proto.isEmpty()) {
      throw new FatalEx("no protocol specified");
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
      throw new FatalEx("unsupported protocol: "+proto);
    }
  }
}
