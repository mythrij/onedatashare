package stork.module.ftp;

import io.netty.buffer.*;

import stork.cred.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

/**
 * A session with an FTP server.
 */
public class FTPSession extends Session<FTPSession, FTPResource> {
  private transient FTPChannel channel;

  // Transient state related to the channel configuration.
  private transient boolean mlstOptsAreSet = false;

  /**
   * Establish an {@code FTPSession} with the endpoint described by {@code uri}
   * and the authentication factor {@code cred}.
   */
  public FTPSession(URI uri, Credential cred) {
    super(new FTPResource(this), uri, cred);
  }

  public Bell<FTPResource> initialize() {
    String user = "anonymous";
    String pass = "";

    // Initialize connection to server.
    ch = new FTPChannel(uri);

    // If the channel is closed by the server, finalize the session.
    ch.onClose().new Promise() {
      public void always() { FTPSession.this.finalize(); }
    };

    // Pull userinfo from URI.
    if (uri.username() != null)
      user = uri.username();
    if (uri.password() != null)
      pass = uri.password();

    // Act depending on the credential type.
    if (credential == null) {
      ch.authorize(user, pass).promise(bell);
    } else if (credential instanceof StorkGSSCred) {
      StorkGSSCred cred = (StorkGSSCred) credential;
      ch.authenticate(cred.data()).new Promise(bell) {
        public void done() {
          ch.authorize(":globus-mapping:", pass).promise(bell);
        }
      };
    } else if (credential instanceof StorkUserinfo) {
      StorkUserinfo cred = (StorkUserinfo) credential;
      user = cred.getUser();
      pass = cred.getPass();
      ch.authorize(user, pass).promise(bell);
    } else {
      // Unsupported credential...
      bell.ring(new IllegalArgumentException());
    }

    // When the bell is rung, ring the returned bell with this session.
    return bell.new PromiseAs<FTPResource>() {
      public FTPResource convert(Object o) { return FTPSession.this; }
    };
  }

  public void finalize() {
    ch.close();
  }
}
