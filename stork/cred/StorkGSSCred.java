package stork.cred;

import stork.util.*;

import java.util.*;
import java.io.*;
import java.net.*;

import org.globus.myproxy.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

// A wrapper for a GSS credential. TODO: Provide additional methods.

public class StorkGSSCred extends StorkCred<GSSCredential> {
  public StorkGSSCred(String user, GSSCredential cred) {
    super(user, cred);
  }

  public String type() {
    return "gss_cert";
  }

  // Return a credential based on certificate bytes.
  public static StorkGSSCred fromBytes(byte[] cred_bytes) {
    try {
      ExtendedGSSManager gm =
        (ExtendedGSSManager) ExtendedGSSManager.getInstance();
      GSSCredential cred = gm.createCredential(
          cred_bytes, ExtendedGSSCredential.IMPEXP_OPAQUE,
          GSSCredential.DEFAULT_LIFETIME, null,
          GSSCredential.INITIATE_AND_ACCEPT);
      return new StorkGSSCred(null, cred);
    } catch (Exception e) {
      throw new FatalEx("couldn't parse certificate", e);
    }
  }

  // Read a certificate from a local file.
  public static StorkGSSCred fromFile(String cred_file) {
    return fromFile(new File(cred_file));
  }

  public static StorkGSSCred fromFile(File cred_file) {
    try {
      FileInputStream fis = new FileInputStream(cred_file);
      byte[] cred_bytes = new byte[(int) cred_file.length()];
      fis.read(cred_bytes);

      return fromBytes(cred_bytes);
    } catch (Exception e) {
      throw new FatalEx("couldn't read certificate file", e);
    }
  }

  // Create a new credential using MyProxy.
  public static StorkGSSCred
    fromMyProxy(String host, int port, String user, String pass)
  throws Exception {
    if (port < 0) port = MyProxy.DEFAULT_PORT;

    MyProxy mp = new MyProxy(host, port);
    GSSCredential cred = mp.get(user, pass, 3600);

    return (cred != null) ? new StorkGSSCred(null, cred) : null;
  } public static StorkGSSCred fromMyProxy(String uri) throws Exception {
    return fromMyProxy(new URI(uri));
  } public static StorkGSSCred fromMyProxy(URI uri) throws Exception {
    String[] ui = StorkUserinfo.split(uri);
    return fromMyProxy(uri.getHost(), uri.getPort(), ui[0], ui[1]);
  }
}
