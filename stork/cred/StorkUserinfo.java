package stork.cred;

import stork.ad.*;
import stork.feather.*;

// A username and password credential.

public class StorkUserinfo extends StorkCred<String[]> {
  public String username;
  public String password;

  public StorkUserinfo() { super("userinfo"); }

  public String[] data() {
    return new String[] { username, password };
  }

  // Return a user/pass pair from a colon-separated string.
  public static String[] split(URI uri) {
    return split(uri.userInfo());
  } public static String[] split(String ui) {
    String u = null, p = null;
    if (ui != null && !ui.isEmpty()) {
      int i = ui.indexOf(':');
      u = (i < 0) ? ui : ui.substring(0,i);
      p = (i < 0) ? "" : ui.substring(i+1);
    } return new String[] { u, p };
  }

  protected Object[] hashables() { return data(); }
}
