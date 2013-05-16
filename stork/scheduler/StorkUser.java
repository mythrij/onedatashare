package stork.scheduler;

import stork.ad.*;
import stork.cred.*;
import stork.util.*;

import java.security.*;

// A user in the StorkCloud system. Each user has their own view of the
// job queue, their own credential manager, and an ad containing their
// user information, such as password hash.

public class StorkUser extends Ad {
  private CredManager cm;

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
  private static final String[] reg_filter =
    { "user_id", "password", "name", "email" };
  private static final String[] reg_require =
    { "user_id", "password" };
  private static final String[] user_filter =
    { "user_id", "pass_hash", "pass_salt", "name", "email" };
  private static final String[] user_require =
    { "user_id" };

  // Create a new user from an ad containing hashed password and other
  // user information.
  // TODO: Verify that password hash is in proper format.
  private StorkUser(Ad ad) {
    super(ad.filter(user_filter));
    System.out.println("The this: "+this);
    String r = require(user_require);

    if (r != null)
      throw new FatalEx("couldn't create user: missing field '"+r+"'");

    // Check and sanitize user_id.
    String user = get("user_id", "").toLowerCase();
    if (user.isEmpty())
      throw new FatalEx("user id was not provided");
    if (!Ad.DECL_ID.matcher(user).matches())
      throw new FatalEx("invalid user id");
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
  // by the same user_id does not already exist. Throws exception if
  // something goes wrong.
  public static StorkUser register(Ad ad) {
    ad = ad.filter(reg_filter);

    // Create user. This checks and sanitizes user_id.
    System.out.println("Before pass: "+ad);
    StorkUser su = new StorkUser(ad);
    su.setPassword(ad.get("password"));

    // The rest of this, until the user has been added, needs to be
    // synchronized on the map.
    Ad map = map();
    synchronized (map) {
      if (lookup(su.get("user_id")) != null)
        throw new FatalEx("this user id is already in use");
      map.put(su.get("user_id"), su);
    } return su;
  }

  // Return the display name of the user. Either their full name or
  // their user id.
  public String name() {
    return get("name", get("user_id"));
  }

  // Check a login request. Returns the user object if the login was
  // successful. Returns null if the user name or password was incorrect.
  // Throws an exception if there was another problem.
  public static StorkUser login(Ad ad) {
    String user = ad.get("user_id");
    String pass = ad.get("password");

    if (user == null || user.isEmpty())
      throw new FatalEx("user_id was not provided");
    if (pass == null || pass.length() < PASS_LEN)
      throw new FatalEx("password is too short");

    StorkUser su = lookup(user);

    if (su == null)
      return null;
    if (!su.has("pass_hash"))
      throw new FatalEx("user's password has not been set");
    if (!su.hash(pass).equals(su.get("pass_hash")))
      return null;
    if (su.getBoolean("disabled"))
      throw new FatalEx("user is disabled");
    return su;
  }

  // Lookup a Stork user by user id. Returns null if the user does
  // not exist.
  public static StorkUser lookup(String id) {
    Ad ad = map().getAd(id);

    if (ad == null)
      return null;
    if (ad instanceof StorkUser)
      return (StorkUser) ad;

    // This happens if server state has been serialized. Might throw
    // exception if the serialization was tampered with.
    StorkUser user = new StorkUser(ad);
    map().put(user.get("user_id"), user);
    return user;
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
    return hash(pass, get("user_id"), get("pass_salt"));
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
