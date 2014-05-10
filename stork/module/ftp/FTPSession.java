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
  transient FTPChannel channel;

  // Transient state related to the channel configuration.
  transient boolean mlstOptsAreSet = false;

  /**
   * Establish an {@code FTPSession} with the endpoint described by {@code uri}
   * and the authentication factor {@code cred}.
   */
  public FTPSession(URI uri, Credential cred) {
    super(new FTPResource(this), uri, cred);
  }

  public Bell<FTPSession> initialize() {
    final Bell bell = new Bell();

    String user = "anonymous";
    String pass = "";

    // Initialize connection to server.
    channel = new FTPChannel(uri);

    // If the channel is closed by the server, finalize the session.
    channel.onClose().new Promise() {
      public void always() { FTPSession.this.finalize(); }
    };

    // Pull userinfo from URI.
    if (uri.username() != null)
      user = uri.username();
    if (uri.password() != null)
      pass = uri.password();

    // Act depending on the credential type.
    if (credential == null) {
      channel.authorize(user, pass).promise(bell);
    } else if (credential instanceof StorkGSSCred) {
      StorkGSSCred cred = (StorkGSSCred) credential;
      channel.authenticate(cred.data()).new Promise(bell) {
        public void done() {
          channel.authorize(":globus-mapping:", pass).promise(bell);
        }
      };
    } else if (credential instanceof StorkUserinfo) {
      StorkUserinfo cred = (StorkUserinfo) credential;
      user = cred.getUser();
      pass = cred.getPass();
      channel.authorize(user, pass).promise(bell);
    } else {
      // Unsupported credential...
      bell.ring(new IllegalArgumentException());
    }

    // When the bell is rung, ring the returned bell with this session.
    return bell.new PromiseAs<FTPSession>() {
      public FTPSession convert(Object o) { return FTPSession.this; }
    };
  }

  public void finalize() {
    channel.close();
  }
}
