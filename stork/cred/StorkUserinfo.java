package stork.cred;

import java.net.*;

// A username and password credential.

public class StorkUserinfo extends StorkCred<String[]> {
  public StorkUserinfo(String stork_user, String user, String pass) {
    super(stork_user, new String[] { user, pass });
  } public StorkUserinfo(String stork_user, String userinfo) {
    super(stork_user, split(userinfo));
  }

  public String type() {
    return "userinfo";
  }

  public StorkUserinfo credential(String user, String pass) {
    credential()[0] = user;
    credential()[1] = pass;
    return this;
  }

  // Return a user/pass pair from a colon-separated string.
  public static String[] split(URI uri) {
    return split(uri.getUserInfo());
  } public static String[] split(String ui) {
    String u = null, p = null;
    if (ui != null && !ui.isEmpty()) {
      int i = ui.indexOf(':');
      u = (i < 0) ? ui : ui.substring(0,i);
      p = (i < 0) ? "" : ui.substring(i+1);
    } return new String[] { u, p };
  }

  public String getUser() {
    return credential()[0];
  }

  public String getPass() {
    return credential()[1];
  }
}
