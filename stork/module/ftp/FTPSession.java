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
  transient boolean mlstOptsAreSet = false;

  /**
   * Establish an {@code FTPSession} with the endpoint described by {@code uri}
   * and the authentication factor {@code cred}.
   */
  public FTPSession(URI uri, Credential cred) {
    super(uri, cred);
  }

  public FTPResource select(Path path) {
    return new FTPResource(this, path);
  }

  public Bell<FTPSession> initialize() {
    return new Bell<Object>() {{
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
        channel.authorize(user, pass).promise(this);
      } else if (credential instanceof StorkGSSCred) {
        StorkGSSCred cred = (StorkGSSCred) credential;
        final String p = pass;
        channel.authenticate(cred.data()).new Promise() {
          public void done() {
            channel.authorize(":globus-mapping:", p).promise(this);
          }
        };
      } else if (credential instanceof StorkUserinfo) {
        StorkUserinfo cred = (StorkUserinfo) credential;
        user = cred.getUser();
        pass = cred.getPass();
        channel.authorize(user, pass).promise(this);
      } else {
        // Unsupported credential...
        ring(new IllegalArgumentException());
      }
    }}.as(FTPSession.this);
  }

  public void finalize() {
    channel.close();
  }

  public static void main(String[] args) {
    URI uri = URI.create("ftp://didclab-ws8/asdasd/asdasd");
    final FTPResource r = new FTPModule().select(uri);
    r.stat().new Promise() {
      public void done(Stat s) {
        System.out.println(s);
      } public void fail(Throwable t) {
        t.printStackTrace();
      } public void always() {
        r.session.close();
      }
    };
  }
}
