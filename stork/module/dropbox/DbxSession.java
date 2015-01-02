package stork.module.dropbox;

import java.util.*;
import java.io.*;

import com.dropbox.core.*;

import stork.feather.*;
import stork.feather.util.*;

public class DbxSession extends Session<DbxSession, DbxResource> {
  static final String APP_KEY = "x";
  static final String APP_SECRET = "x";

  DbxClient client;

  public DbxSession(URI uri, Credential cred) {
    super(uri, cred);
  }

  public DbxResource select(Path path) {
    return new DbxResource(this, path);
  }

  public Bell<DbxSession> initialize() {
    // If an OAuth token is provided, use it. TODO
    /*
    if (credential != null) {
      Credential<String> cred = (Credential<String>) credential;
      return credential.data().new Promise() {
        public void done(String token) {
          client = new DbxClient(config, accessToken);
        }
      }.as(this);
    }
    */

    // Otherwise, redirect user to OAuth page.
    return new ThreadBell<DbxSession>() {
      public DbxSession run() throws Exception {
        DbxRequestConfig cfg =
          new DbxRequestConfig("StorkCloud", Locale.getDefault().toString());
        String url = new DbxWebAuthNoRedirect(
          cfg, new DbxAppInfo(APP_KEY, APP_SECRET)).start();
        //throw new RuntimeException("Go to: "+url);
        System.out.println("Go to: "+url);
        //System.out.print("Enter key: ");
        //String key = new BufferedReader(new InputStreamReader(System.in)).readLine();
        String key = "x";
        client = new DbxClient(cfg, key);
        return DbxSession.this;
      }
    }.start();
  }
}
