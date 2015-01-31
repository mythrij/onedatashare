package stork.core.handlers;

import java.util.*;

import stork.core.server.*;
import stork.feather.*;
import stork.feather.errors.*;
import stork.util.*;

import static stork.core.server.User.PASS_LEN;

/** Handle user registration, login, and settings. */
public class UserHandler extends Handler<UserRequest> {
  public void handle(final UserRequest req) {
    Bell bell = new Bell();
    if (req.action == null) {
      throw new RuntimeException("No action provided.");
    }

    if (req.action.equals("register")) {
      UserRegistration reg = req.marshalInto(new UserRegistration());
      final User user = server.createUser(reg.validate());
      user.sendValidationMail().new Promise() {
        public void done() {
          server.saveUser(user);
          stork.util.Log.info("Registering user: ", user.email);
          server.dumpState();
        } public void fail(Throwable t) {
          Log.warning("Failed registration: ", t);
          req.ring(new RuntimeException("Registration failed."));
        }
      };
      req.ring(new Object());
    }

    else if (req.action.equals("login")) try {
      UserLogin log = req.marshalInto(new UserLogin());
      User user = log.login();
      req.ring(user.getLoginCookie());
    } catch (final Exception e) {
      Log.warning("Failed login: ", e);

      // Delay if login credentials are wrong.
      Bell.timerBell(1).new Promise() {
        public void done() { req.ring(e); }
      };
    }

    else if (req.action.equals("history")) {
      req.assertLoggedIn();
      if (req.uri != null) {
        req.assertMayChangeState();
        req.user().addHistory(req.uri);
      }
      req.ring(req.user().history);
    }

    else if (req.action.equals("validate")) {
      UserValidator uv = req.marshalInto(new UserValidator());
      if (uv.validate()) {
        throw new Redirect("/#/validate");
      } else {
        throw new Redirect("/#/validateError");
      }
    }

    else {
      throw new RuntimeException("Invalid action.");
    }
  }

  /** A registration request. */
  public static class UserRegistration extends Request {
    public String email;
    public String password;

    /** Validate the registration form. */
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

/** Form used to check user credentials. */
class UserLogin extends Request {
  public String email;
  public String password;
  public String hash;

  /** Attempt to log in with the given information. */
  public User login() {
    User.Cookie cookie = new User.Cookie(server);

    cookie.email = email;
    cookie.password = password;
    cookie.hash = hash;

    User user = cookie.login();

    if (!user.validated)
      throw new User.NotValidatedException();

    return user;
  }
}

/** Form used to validate a user's account. */
class UserValidator extends Request {
  public String user;
  public String token;

  /** Try to validate the user. */
  public boolean validate() {
    if (user == null || token == null)
      return false;
    User realUser = server.findUser(user);
    return realUser.validate(token);
  }
}
