package stork.core.server;

import java.net.IDN;
import java.security.*;
import java.util.*;

import stork.core.*;
import stork.cred.*;
import stork.feather.*;
import stork.scheduler.*;
import stork.util.*;

/**
 * A registered Stork user. Each user has their own view of the job queue,
 * transfer credential manager, login credentials, and user info.
 */
public class User {
  public String email;
  public String hash;
  public String salt;
  public boolean validated = true;

  public LinkedList<URI> history;
  public Map<String,StorkCred> credentials;
  public List<Job> jobs;

  /** The minimum allowed password length. */
  public static final int PASS_LEN = 6;

  // Create an anonymous user. Only instantiate one of these, and do not
  // register it in any UserMap.
  private User() { email = "anonymous"; }

  public User(String email, String password) {
    this.email = email;
    setPassword(password);
  }

  /** Check if the given password is correct for this user. */
  public synchronized boolean checkPassword(String password) {
    return hash(password).equals(hash);
  }

  /** Set the password for this user. Checks password length and hashes. */
  public synchronized void setPassword(String pass) {
    if (pass == null || pass.isEmpty())
      throw new RuntimeException("No password was provided.");
    if (pass.length() < PASS_LEN)
      throw new RuntimeException("Password must be "+PASS_LEN+"+ characters.");
    salt = salt();
    hash = hash(pass);
  }

  /** Get an object containing information to return on login. */
  public Object getLoginCookie() {
    return new Object() {
      String email = User.this.email;
      String hash = User.this.hash;
      List history = User.this.history;
    };
  }

  /** Add a URL to a user's history. */
  public synchronized void addHistory(URI u) {
    if (!isAnonymous() && Config.global.max_history > 0) try {
      history.remove(u);
      while (history.size() > Config.global.max_history)
        history.removeLast();
      history.addFirst(u);
    } catch (Exception e) {
      // Just don't add it.
    }
  }

  /** Create an anonymous user. */
  public static User anonymous() {
    return new User() {
      public boolean isAnonymous() { return true; }
    };
  }

  /** Check if a user is anonymous. */
  public boolean isAnonymous() { return false; }

  /** Normalize an email string for comparison. */
  public static String normalizeEmail(String email) {
    String[] parts = email.split("@");
    if (parts.length != 2)
      throw new RuntimeException("Invalid email address.");
    return parts[0].toLowerCase()+"@"+IDN.toASCII(parts[1]).toLowerCase();
  }

  /** Get the normalized email address of this user. */
  public String normalizedEmail() {
    return normalizeEmail(email);
  }

  /** Generate a random salt using a secure random number generator. */
  public static String salt() { return salt(24); }

  /** Generate a random salt using a secure random number generator. */
  public static String salt(int len) {
    byte[] b = new byte[len];
    SecureRandom random = new SecureRandom();
    random.nextBytes(b);
    return StorkUtil.formatBytes(b, "%02x");
  }

  /** Hash a password with this user's salt. */
  public String hash(String pass) {
    return hash(pass, salt);
  }

  /** Hash a password with the given salt. */
  public static String hash(String pass, String salt) {
    try {
      String saltpass = salt+'\n'+pass;
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] digest = saltpass.getBytes("UTF-8");

      // Run the digest for two rounds.
      for (int i = 0; i < 2; i++)
        digest = md.digest(digest);

      return StorkUtil.formatBytes(digest, "%02x");
    } catch (Exception e) {
      throw new RuntimeException("Couldn't hash password.");
    }
  }
}
