package stork.cred;

import java.util.*;
import java.io.*;

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
  public static StorkGSSCred fromBytes(byte[] cred_bytes)
  throws GSSException {
    ExtendedGSSManager gm =
      (ExtendedGSSManager) ExtendedGSSManager.getInstance();
    GSSCredential cred = gm.createCredential(
        cred_bytes, ExtendedGSSCredential.IMPEXP_OPAQUE,
        GSSCredential.DEFAULT_LIFETIME, null,
        GSSCredential.INITIATE_AND_ACCEPT);
    return new StorkGSSCred(null, cred);
  }

  // Read a certificate from a local file.
  public static StorkGSSCred fromFile(File cred_file)
  throws IOException, GSSException {
    FileInputStream fis = new FileInputStream(cred_file);
    byte[] cred_bytes = new byte[(int) cred_file.length()];
    fis.read(cred_bytes);

    return fromBytes(cred_bytes);
  }
}
