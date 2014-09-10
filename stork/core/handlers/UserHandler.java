package stork.core.handlers; 

import java.util.*;

import stork.core.server.*;
import stork.feather.*;

import static stork.core.server.User.PASS_LEN;

/** Handle user registration, login, and settings. */
public class UserHandler extends Handler<UserRequest> {
  public void handle(UserRequest req) {
    Bell bell = new Bell();
    if (req.action == null) {
      throw new RuntimeException("No action provided.");
    } if (req.action.equals("register")) {
      UserRegistration reg = req.marshalInto(new UserRegistration());
      User user = server.createAndInsertUser(reg.validate());
      stork.util.Log.info("Registering user: ", user.email);
      server.dumpState();
      req.ring(user.getLoginCookie());
    } else if (req.action.equals("login")) {
      UserLogin log = req.marshalInto(new UserLogin());
      User user = log.login();
      req.ring(user.getLoginCookie());
    } else if (req.action.equals("history")) {
      req.assertLoggedIn();
      if (req.uri != null)
        req.user.addHistory(req.uri);
      req.ring(req.user.history);
    } else {
      throw new RuntimeException("Invalid action.");
    }
  }

  /** A registration request. */
  public static class UserRegistration extends Request {
    public String email;
    public String password;

    /** Validate and create a new user. */
    public UserRegistration validate() {
      if (email == null || (email = email.trim()).isEmpty())
        throw new RuntimeException("No email address provided.");
      if (password == null || password.isEmpty())
        throw new RuntimeException("No password provided.");
      if (password.length() < PASS_LEN)
        throw new RuntimeException("Password must be "+PASS_LEN+"+ characters.");
      if (server.users.containsKey(User.normalizeEmail(email)))
        throw new RuntimeException("This email is already in use.");
      return this;
    }
  }
}

class UserRequest extends Request {
  String action;
  URI uri;  // Used for history command.
}

class UserLogin extends Request {
  public String email;
  public String password;
  public String hash;

  /** Attempt to log in with the given information. */
  public User login() {
    if (email == null || (email = email.trim()).isEmpty())
      throw new RuntimeException("No email address provided.");
    if (hash == null && (password == null || password.isEmpty()))
      throw new RuntimeException("No password provided.");
    User user = server.users.get(User.normalizeEmail(email));
    if (user == null)
      throw new RuntimeException("Invalid username or password.");
    if (hash == null)
      hash = user.hash(password);
    if (!hash.equals(user.hash))
      throw new RuntimeException("Invalid username or password.");
    if (!user.validated)
      throw new RuntimeException("This account has not been validated.");
    return user;
  }
}
