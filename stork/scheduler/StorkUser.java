package stork.scheduler;

import stork.ad.*;
import stork.cred.*;
import stork.util.*;
import static stork.util.StorkUtil.splitCSV;

import java.util.*;
import java.security.*;

// A user in the StorkCloud system. Each user has their own view of the
// job queue, their own credential manager, and an ad containing their
// user information, such as password hash.

public class StorkUser {
  private String user_id   = null;
  private String pass_hash = null;
  private String pass_salt = null;
  private String name      = null;
  private String email     = null;

  private transient CredManager cm;
  private transient JobQueue queue;

  // The global user id map.
  private static Map<String, StorkUser> user_map = null;

  // Get/set the global user map.
  public static synchronized Map<String, StorkUser> map() {
    if (user_map == null)
      user_map = new HashMap<String, StorkUser>();
    return user_map;
  }

  // The minimum password length.
  public static final int PASS_LEN = 6;

  // Create a user with the given id.
  public StorkUser(String uid) {
    uid = normalize(uid);
    if (uid == null)
      throw new FatalEx("no user ID was provided");
    user_id = uid;
  }

  // Create a user from an ad.
  public static StorkUser unmarshal(Ad ad) {
    StorkUser u = new StorkUser(ad.get("user_id"));
    if (ad.has("pass_hash")) {
      u.pass_hash = ad.get("pass_hash");
    } else if (ad.has("password")) {
      u.setPassword(ad.get("password"));
    } else {
      throw new FatalEx("no password was provided");
    } return u;
  }

  // Return whether or not a user name is allowed.
  public static boolean allowedUserId(String s) {
    return true;
  }

  // Normalize a user_id by lowercasing it and making sure it contains
  // valid characters and doesn't start with a number. Returns null if
  // it's a bad user name.
  public static String normalize(String s) {
    if (s == null || s.isEmpty() || !allowedUserId(s))
      return null;
    return s.toLowerCase();
  }

  // Set the password for this user. Checks password length and handles
  // hashing and whatnot. Throws if there's an error.
  public synchronized void setPassword(String pass) {
    if (pass == null)
      throw new FatalEx("no password was provided");
    if (pass.length() < PASS_LEN)
      throw new FatalEx("password shorter than "+PASS_LEN+" characters");

    // TODO: Randomly generate salt instead of doing this.
    String salt = generateSalt();
    pass_salt = salt;
    pass_hash = hash(pass);
  }

  // Register a new user and add them to the login map, only if a user
  // by the same user_id does not already exist. If a user attempts to
  // register with the same user_id and password as an existing user,
  // return that user object, assuming it's someone who forgot they had
  // an account already. Throws exception if something goes wrong.
  public static StorkUser register(Ad ad) {
    // Create user. This checks and sanitizes user_id.
    StorkUser su = new StorkUser(ad.get("user_id"));
    su.setPassword(ad.get("password"));

    // The rest of this, until the user has been added, needs to be
    // synchronized on the map.
    synchronized (map()) {
      if (map().containsKey(su.user_id)) try {
        return login(ad);
      } catch (Exception e) {
        throw new FatalEx("this user id is already in use");
      } map().put(su.user_id, su);
    } return su;
  }

  // Return the display name of the user. Either their full name or
  // their user id.
  public String name() {
    return (name != null) ? name : user_id;
  }

  // Get the queue for this user.
  public JobQueue queue() {
    return queue;
  }

  // A user equals another user if their user_ids and their pass_hashes
  // are the same.
  public boolean equals(Object o) {
    if (o instanceof StorkUser) {
      StorkUser u = (StorkUser) o;
      return user_id.equals(u.user_id) && pass_hash.equals(u.pass_hash);
    } return false;
  }

  public int hashCode() {
    int h = user_id.hashCode();
    if (pass_hash != null)
      h *= pass_hash.hashCode();
    return h;
  }

  // Check a login request. Returns the user object if the login was
  // successful. Throws an exception if there's a problem.
  public static StorkUser login(Ad ad) {
    if (!ad.has("user_id"))
      throw new FatalEx("missing field: user_id");

    String user = normalize(ad.get("user_id"));
    String pass = ad.get("password");
    String hash = ad.get("pass_hash");

    if (pass == null && hash == null)
      throw new FatalEx("no password or password hash supplied");

    StorkUser su = lookup(user);

    if (su == null)
      throw new FatalEx("invalid username or password");
    if (su.pass_hash == null)
      throw new FatalEx("user is disabled");

    if (hash == null)
      hash = su.hash(pass);

    if (!hash.equals(su.pass_hash))
      throw new FatalEx("invalid username or password");
    return su;
  }

  // Lookup a Stork user by user id. Returns null if the user does
  // not exist.
  public static StorkUser lookup(String id) {
    if ((id = normalize(id)) == null)
      return null;
    return map().get(id);
  }

  // Generate a random salt using a secure random number generator.
  public static String generateSalt() {
    return generateSalt(12);
  } public static String generateSalt(int len) {
    byte[] b = new byte[len];
    SecureRandom random = new SecureRandom();
    random.nextBytes(b);
    return StorkUtil.formatBytes(b, "%02x");
  }

  // Hash a password using salt.
  // TODO: Check password constraints.
  public String hash(String pass) {
    return hash(pass, user_id, pass_salt);
  } public static String hash(String pass, String user, String salt) {
    try {
      String saltpass = salt+'\n'+user+'\n'+pass;
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] digest = saltpass.getBytes("UTF-8");

      // Run the digest for two rounds.
      for (int i = 0; i < 2; i++)
        digest = md.digest(digest);

      return StorkUtil.formatBytes(digest, "%02x");
    } catch (Exception e) {
      throw new FatalEx("couldn't hash password");
    }
  }
}
