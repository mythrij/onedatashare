package stork.scheduler;

import stork.ad.*;
import stork.cred.*;
import stork.util.*;
import static stork.util.StorkUtil.splitCSV;

import java.security.*;

// A user in the StorkCloud system. Each user has their own view of the
// job queue, their own credential manager, and an ad containing their
// user information, such as password hash.

public class StorkUser extends Ad {
  private CredManager cm;
  private JobQueue queue;

  // The global user id map.
  private static Ad user_map = null;

  // Get/set the global user map.
  public static synchronized void map(Ad map) {
    user_map = map;
  } public static synchronized Ad map() {
    if (user_map == null)
      user_map = new Ad();
    return user_map;
  }

  // The minimum password length.
  public static final int PASS_LEN = 6;

  // Filters/requirements for registration and StorkUser state.
  private static final String reg_require =
    "user_id, password";
  private static final String reg_optional =
    "name, email, contact, institution";
  private static final String user_require =
    "user_id";
  private static final String user_optional =
    "pass_hash, pass_salt, "+reg_optional;

  // Normalize a user_id by lowercasing it and making sure it contains
  // valid characters and doesn't start with a number. Returns null if
  // it's a bad user name.
  public static String normalize(String s) {
    if (s == null || s.isEmpty() || !Ad.DECL_ID.matcher(s).matches())
      return null;
    return s.toLowerCase();
  }

  // Create a new user from an ad containing hashed password and other
  // user information.
  // TODO: Verify that password hash is in proper format.
  private StorkUser(Ad ad) {
    super(ad.model(user_require, user_optional));

    // Check and sanitize user_id.
    String user = normalize(get("user_id"));
    if (user == null)
      throw new FatalEx("user id was not provided");
    put("user_id", user);
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
    put("pass_salt", salt);
    put("pass_hash", hash(pass));
  }

  // Register a new user and add them to the login map, only if a user
  // by the same user_id does not already exist. If a user attempts to
  // register with the same user_id and password as an existing user,
  // return that user object, assuming it's someone who forgot they had
  // an account already. Throws exception if something goes wrong.
  public static StorkUser register(Ad ad) {
    ad = ad.model(reg_require, reg_optional);

    // Create user. This checks and sanitizes user_id.
    StorkUser su = new StorkUser(ad);
    su.setPassword(ad.get("password"));

    // The rest of this, until the user has been added, needs to be
    // synchronized on the map.
    Ad map = map();
    synchronized (map) {
      if (map.has(su.user_id())) try {
        return login(ad);
      } catch (Exception e) {
        throw new FatalEx("this user id is already in use");
      } map.put(su.user_id(), su);
    } return su;
  }

  // Get the user_id from the underlying ad.
  public String user_id() {
    return get("user_id", "");
  }

  // Return the display name of the user. Either their full name or
  // their user id.
  public String name() {
    return get("name", user_id());
  }

  // Get the queue for this user.
  public JobQueue queue() {
    return queue;
  }

  // A user equals another user if their user_ids and their pass_hashes
  // are the same.
  public boolean equals(Object o) {
    if (o instanceof Ad)
      return ((Ad)o).filter("user_id", "pass_hash").equals(
                this.filter("user_id", "pass_hash"));
    return false;
  }

  public int hashCode() {
    return filter("user_id", "pass_hash").hashCode();
  }

  // Check a login request. Returns the user object if the login was
  // successful. Throws an exception if there's a problem.
  public static StorkUser login(Ad ad) {
    ad.model("user_id", null);

    String user = normalize(ad.get("user_id"));
    String pass = ad.get("password");
    String hash = ad.get("pass_hash");

    if (pass == null && hash == null)
      throw new FatalEx("no password or password hash supplied");

    StorkUser su = lookup(user);

    if (su == null)
      throw new FatalEx("invalid username or password");
    if (!su.has("pass_hash"))
      throw new FatalEx("user's password has not been set");

    if (hash == null)
      hash = su.hash(pass);

    if (!hash.equals(su.get("pass_hash")))
      throw new FatalEx("invalid username or password");
    if (su.getBoolean("disabled"))
      throw new FatalEx("user is disabled");
    return su;
  }

  // Lookup a Stork user by user id. Returns null if the user does
  // not exist.
  public static StorkUser lookup(String id) {
    if ((id = normalize(id)) == null)
      return null;
    Ad ad = map().getAd(id);

    if (ad == null)
      return null;
    if (ad instanceof StorkUser)
      return (StorkUser) ad;

    // This happens if server state has been serialized. Might throw
    // exception if the serialization was tampered with.
    try {
      StorkUser user = new StorkUser(ad);
      map().put(user.user_id(), user);
      return user;
    } catch (Exception e) {
      return null;
    }
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
    return hash(pass, user_id(), get("pass_salt"));
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
