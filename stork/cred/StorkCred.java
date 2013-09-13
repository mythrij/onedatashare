package stork.cred;

import stork.util.*;
import stork.ad.*;

// An interface for credentials.

public abstract class StorkCred<O> {
  public String name;
  public final String type;
  public String user_id = null;
  public Watch time = new Watch();

  public StorkCred(String type) {
    this.type = type;
  }

  public static StorkCred<?> unmarshal(Ad ad) {
    return create(ad);
  }

  // Create a credential from an ad.
  // TODO: This is very hacky and can be done better.
  public static StorkCred<?> create(Ad ad) {
    String type = ad.get("type", "").toLowerCase();
    String own  = ad.get("user_id");
    String name = ad.get("name");
    if (type.isEmpty()) {
      throw new RuntimeException("no credential type provided");
    } if (type.equals("gss-cert")) {
      return new StorkGSSCred(ad).owner(own).name(name);
    } if (type.equals("userinfo")) {
      return new StorkUserinfo(ad).owner(own).name(name);
    } throw new RuntimeException("invalid credential type: "+type);
  }

  // Get/set the id of the user this credential is for.
  public StorkCred<?> owner(String o) {
    user_id = o;
    return this;
  } public String owner() {
    return user_id;
  }

  public String type() {
    return type;
  }

  // Get or set the name.
  public final String name() {
    if (name == null)
      return "(unnamed)";
    return name;
  } public StorkCred<O> name(String s) {
    name = s;
    return this;
  }

  // Get the raw credential object.
  public abstract O credential();

  // Get the duration of the credential in milliseconds.
  public long duration() {
    return time.elapsed();
  }

  // Get an ad suitable for showing to users. It should not include
  // sensitive information.
  public Ad getAd() {
    return new Ad("name", name)
             .put("type", type)
             .put("user_id", user_id)
             .put("timer", time);
  }
}
