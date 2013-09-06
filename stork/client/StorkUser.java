package stork.client;

import stork.*;
import stork.ad.*;

import java.io.*;
import java.util.*;

// User management stuff, like logging in, registering, or changing password.
// It's hilarious that there's a class called this already and we reference
// it in here. Use its fully qualified name: stork.scheduler.StorkUser.

public class StorkUser extends StorkClient {
  private String user_id;

  public StorkUser() {
    super("user");

    args = new String[] { "<user_id>" };
    desc = new String[] {
      "Log in to a Stork server or register as a new user. "+
      "You will be prompted to enter your password.",
      "This command is not completely implemented, and will only verify "+
      "that a user ID and password is correct."
    };
    add('r', "register", "register as a new user");
    add('t', "time", "length of time to remain logged in (NOT IMPLEMENTED)");
  }

  public void parseArgs(String[] args) {
    assertArgsLength(args, 1);
    user_id = stork.scheduler.StorkUser.normalize(args[0]);
  }

  public Ad fillCommand(Ad ad) {
    Console c = System.console();

    // Make sure we're on a console.
    if (c == null) throw new RuntimeException(
      "command must be executed on an interactive console");

    // Read the password from the command line.
    // TODO: Haha, look at this security theater. Hash instead of
    // making string.
    char[] b = c.readPassword("Password: ");
    String password = new String(b);
    Arrays.fill(b, '\000');

    if (env.getBoolean("register"))
      ad.put("action", "register");
    ad.put("user_id", user_id);
    return ad.put("password", password);
  }
      
  public void handle(Ad ad) {
    if (!ad.has("error"))
      System.out.println("Logged in as: "+ad.get("user_id"));
  }
}
