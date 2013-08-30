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
  public String user_id   = null;
  public String pass_hash = null;
  public String pass_salt = null;
  public String name      = null;
  public String email     = null;

  private transient CredManager cm;
  private transient JobQueue queue;

  // The minimum password length.
  public static final int PASS_LEN = 6;

  // Create a user with the given id.
  public StorkUser(String uid) {
    uid = normalize(uid);
    if (uid == null)
      throw new RuntimeException("no user ID was provided");
    user_id = uid;
  }

  // Create a user from an ad.
  public static StorkUser unmarshal(Ad ad) {
    StorkUser u = new StorkUser(ad.get("user_id"));
    if (ad.has("pass_hash") && ad.has("pass_salt")) {
      u.pass_hash = ad.get("pass_hash");
      u.pass_salt = ad.get("pass_salt");
    } else if (ad.has("password")) {
      u.setPassword(ad.get("password"));
    } else {
      throw new RuntimeException("no password was provided");
    } return u;
  }

  // A class which can be used to store users.
  @SuppressWarnings({"serial"})
  public static class UserMap extends HashMap<String, StorkUser> {
    // TODO: Remove this terrible shit later when ads handle generics.
    public static UserMap unmarshal(Ad ad) {
      UserMap u = new UserMap();

      for (String s : ad.keySet())
        u.put(s, ad.getAd(s).unmarshal(new StorkUser(s)));
      return u;
    }

    // Add a user to this user map based on a registration ad.
    public synchronized StorkUser register(Ad ad) {
      // Create user. This checks and sanitizes user_id.
      StorkUser su = new StorkUser(ad.get("user_id"));
      su.setPassword(ad.get("password"));

      // The rest of this, until the user has been added, needs to be
      // synchronized on the map.
      if (containsKey(su.user_id)) try {
        return login(ad);
      } catch (Exception e) {
        throw new RuntimeException("this user id is already in use");
      } put(su.user_id, su);
      return su;
    }

    // Check a login request. Returns the user object if the login was
    // successful. Throws an exception if there's a problem.
    public StorkUser login(Ad ad) {
      String user = normalize(ad.get("user_id"));
      if (user == null)
        throw new RuntimeException("missing field: user_id");
      user = user.trim();
      if (user.isEmpty())
        throw new RuntimeException("user_id is empty");

      String pass = ad.get("password");
      String hash = ad.get("pass_hash");

      if (pass == null && hash == null)
        throw new RuntimeException("no password or password hash supplied");

      StorkUser su = lookup(user);

      if (su == null)
        throw new RuntimeException("invalid username or password");
      if (su.pass_hash == null)
        throw new RuntimeException("user is disabled");

      if (hash == null)
        hash = su.hash(pass);

      if (!hash.equals(su.pass_hash))
        throw new RuntimeException("invalid username or password");
      return su;
    }

    // Lookup a Stork user by user id. Returns null if the user does
    // not exist.
    public StorkUser lookup(String id) {
      if ((id = normalize(id)) == null)
        return null;
      return get(id);
    }
  }

  // Return whether or not a user name is allowed. For now allow any
  // user name.
  public static boolean allowedUserId(String s) {
    return true;
  }

  // Normalize a user_id by lowercasing it and making sure it's not an
  // empty string. Returns null if it's invalid.
  public static String normalize(String s) {
    if (s == null || s.isEmpty())
      return null;
    return s.toLowerCase().trim();
  }

  // Set the password for this user. Checks password length and handles
  // hashing and whatnot. Throws if there's an error.
  public synchronized void setPassword(String pass) {
    if (pass == null)
      throw new RuntimeException("no password was provided");
    if (pass.length() < PASS_LEN)
      throw new RuntimeException("password shorter than "+PASS_LEN+" characters");

    // TODO: Randomly generate salt instead of doing this.
    String salt = generateSalt();
    pass_salt = salt;
    pass_hash = hash(pass);
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
      h ^= pass_hash.hashCode();
    return h;
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
      throw new RuntimeException("couldn't hash password");
    }
  }
}
