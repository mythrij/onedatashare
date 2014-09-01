package stork.cred;

import stork.ad.*;
import stork.feather.*;
import stork.util.*;

import java.util.*;
import java.io.*;

import org.globus.myproxy.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

/** A wrapper for a GSS credential. */
public class StorkGSSCred extends StorkCred<GSSCredential> {
  private String proxy_string = null;
  private int    proxy_life   = 3600;
  private String myproxy_user = null;
  private String myproxy_pass = null;
  private String myproxy_host = null;
  private int    myproxy_port = -1;
  private transient GSSCredential credential = null;

  public StorkGSSCred() { super("gss"); }

  /** Lazily instantiate this credential. */
  // TODO: This should somehow be made asynchronous.
  public GSSCredential data() {
    return (credential != null) ? credential : initialize();
  }

  // Call this after unmarshalling to instantiate credential from stored
  // information. Can be called again to refresh the credential as well.
  private GSSCredential initialize() {
    try {
      return credential = initCred();
    } catch (Exception e) {
      throw new RuntimeException(type+": "+e.getMessage());
    }
  }

  private GSSCredential initCred() throws Exception {
    if (proxy_life < 3600) {
      throw new Exception("Cred lifetime must be at least one hour.");
    } if (myproxy_user != null) {
      if (myproxy_port <= 0 || myproxy_port > 0xFFFF)
        myproxy_port = MyProxy.DEFAULT_PORT;
      MyProxy mp = new MyProxy(myproxy_host, myproxy_port);
      return mp.get(myproxy_user, myproxy_pass, proxy_life);
    } if (proxy_string != null) {
      byte[] b = proxy_string.getBytes("UTF-8");
      ExtendedGSSManager gm =
        (ExtendedGSSManager) ExtendedGSSManager.getInstance();
      return gm.createCredential(
        b, ExtendedGSSCredential.IMPEXP_OPAQUE, GSSCredential.DEFAULT_LIFETIME, null,
        GSSCredential.INITIATE_AND_ACCEPT);
    } else {
      throw new Exception("Not enough information.");
    }
  }

  // Read a certificate from a local file.
  public static StorkGSSCred fromFile(String cred_file) {
    return fromFile(new File(cred_file));
  } public static StorkGSSCred fromFile(File cred_file) {
    StorkGSSCred cred = new StorkGSSCred();
    cred.proxy_string = StorkUtil.readFile(cred_file);
    return cred;
  }
}
