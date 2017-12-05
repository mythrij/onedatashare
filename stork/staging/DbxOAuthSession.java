package stork.staging;

import java.util.*;

/** Include dropbox sdk. */
import com.dropbox.core.*;

import stork.core.*;
import stork.cred.*;

/** OAuth wrapper for Dropbox. */
public class DbxOAuthSession extends OAuthSession {
  /** The URI to go to to finish authentication. */
  final static String finishURI;
  /** Dropbox secret keys. Set from config entries. */
  private final static DbxAppInfo secrets;

  public static class DropboxConfig {
    public String key, secret, redirect;
  }

  // Get Dropbox secrets from global config.
  static {
    DropboxConfig c = Config.global.dropbox;
    if (c != null && c.key != null && c.secret != null && c.redirect != null) {
      secrets = new DbxAppInfo(c.key, c.secret);
      finishURI = c.redirect;
    } else {
      secrets = null;
      finishURI = null;
    }
  }

  /** Created after start() is called. */
  private DbxWebAuth auth;
  /** Used by Dropbox SDK. Should be set to user's locale, not ours. */
  private DbxRequestConfig config =
          DbxRequestConfig.newBuilder("StorkCloud").build();
  /**
   * Used by Dropbox SDK to store the session key. The "key" member is part of
   * the OAuthSession base class.
   */
  private DbxSessionStore sessionStore = new DbxSessionStore() {
    public void clear() { set(null); }
    public String get() { return key; }
    public void set(String s) { key = s; }
  };

  /** Start the handshake. Return Dropbox OAuth URL. */
  public synchronized String start() {
    if (secrets == null) {
      throw new RuntimeException("Dropbox OAuth is disabled.");
    } if (auth != null) {
      throw new IllegalStateException("Don't call this twice.");
    } try {
      // Create a new DbxWebAuth object using the StorkCloud DbxRequestConfig config and
      // secrets
      auth = new DbxWebAuth(config, secrets);
      // Authorize the DbxWebAuth auth as well as redirect the user to the finishURI, done this way to appease OAuth 2.0
      return auth.authorize(DbxWebAuth.Request.newBuilder().withRedirectUri(finishURI, sessionStore).build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Finish the handshake. */
  public synchronized StorkOAuthCred finish(String token) {
    // Do this to appease the Dropbox SDK.
    Map<String,String[]> map = new HashMap();
    map.put("state", new String[] {this.key});
    map.put("code", new String[] {token});

    try {
      DbxAuthFinish finish = auth.finishFromRedirect(finishURI, sessionStore, map);
      StorkOAuthCred cred = new StorkOAuthCred(finish.getAccessToken());
      cred.name = "Dropbox";
      return cred;
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
