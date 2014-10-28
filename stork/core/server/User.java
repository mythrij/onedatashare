package stork.core.server;

import java.net.IDN;
import java.security.*;
import java.util.*;

import stork.ad.*;
import stork.core.*;
import stork.cred.*;
import stork.feather.*;
import stork.scheduler.*;
import stork.util.*;

/**
 * A registered Stork user. Each user has their own view of the job queue,
 * transfer credential manager, login credentials, and user info.
 */
public abstract class User {
  public String email;
  public String hash;
  public String salt;
  public boolean validated = true;

  public LinkedList<URI> history = new LinkedList<URI>();
  public Map<String,StorkCred> credentials = new HashMap<String,StorkCred>();

  private ArrayList<UUID> jobs = new ArrayList<UUID>();

  /** Basic user login cookie. */
  public static class Cookie {
    public String email;
    public String hash;
    public String password;
    private transient Server server;
    
    protected Cookie() { }

    public Cookie(Server server) { this.server = server; }

    /** Can be overridden by subclasses. */
    public Server server() { return server; }

    /** Attempt to log in with the given information. */
    public User login() {
      if (email == null || (email = email.trim()).isEmpty())
        throw new RuntimeException("No email address provided.");
      if (hash == null && (password == null || password.isEmpty()))
        throw new RuntimeException("No password provided.");
      User user = server().users.get(User.normalizeEmail(email));
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

  // A job owned by this user.
  private class UserJob extends Job {
    public User user() { return User.this; }
    public Server server() { return User.this.server(); }
  }

  /** The minimum allowed password length. */
  public static final int PASS_LEN = 6;

  /** Create an anonymous user. */
  public User() { }

  /** Create a user with the given email and password. */
  public User(String email, String password) {
    this.email = email;
    setPassword(password);
  }

  /** Get the server this user belongs to. */
  public abstract Server server();

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
  public Cookie getLoginCookie() {
    Cookie cookie = new Cookie(server());
    cookie.email = email;
    cookie.hash = hash;
    return cookie;
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

  /** Check if a user is anonymous. */
  public boolean isAnonymous() { return email == null; }

  /** Normalize an email string for comparison. */
  public static String normalizeEmail(String email) {
    if (email == null)
      return null;
    String[] parts = email.split("@");
    if (parts.length != 2)
      throw new RuntimeException("Invalid email address.");
    return parts[0].toLowerCase()+"@"+IDN.toASCII(parts[1]).toLowerCase();
  }

  /** Get the normalized email address of this user. */
  public String normalizedEmail() {
    return normalizeEmail(email);
  }

  /** Save a {@link Job} to this {@code User}'s {@code jobs} list. */
  public synchronized Job saveJob(Job job) {
    job.owner = normalizedEmail();
    jobs.add(job.uuid());
    job.jobId(jobs.size());
    return job;
  }

  /** Get one of this user's jobs by its ID. */
  public synchronized Job getJob(int id) {
    try {
      UUID uuid = jobs.get(id);
      return server().findJob(uuid);
    } catch (Exception e) {
      throw new RuntimeException("No job with that ID.");
    }
  }

  /** Get a list of actual jobs owned by the user. */
  public synchronized List<Job> jobs() {
    // FIXME: Inefficient...
    List<Job> list = new LinkedList<Job>();
    for (int i = 0; i < jobs.size(); i++)
      list.add(getJob(i));
    return list;
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
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] digest = saltpass.getBytes("UTF-8");

      // Run the digest for three rounds.
      for (int i = 0; i < 3; i++)
        digest = md.digest(digest);

      return StorkUtil.formatBytes(digest, "%02x");
    } catch (Exception e) {
      throw new RuntimeException("Couldn't hash password.");
    }
  }
}
