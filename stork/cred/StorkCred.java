package stork.cred;

import stork.ad.*;
import stork.scheduler.*;
import stork.util.*;

// An interface for credentials.

public abstract class StorkCred<O> {
  public String name;
  public final String type;
  public Watch time = new Watch();

  public StorkCred(String type) {
    this.type = type;
  }

  // Retrieve a credential by UUID.
  public static StorkCred unmarshal(String uuid) {
    return StorkScheduler.instance().creds.getCred(uuid);
  }

  // Create an anonymous credential.
  public static StorkCred unmarshal(Ad ad) {
    return create(ad);
  }

  // Create a credential from an ad.
  public static StorkCred<?> create(Ad ad) {
    String type = ad.get("type", "").toLowerCase();
    String name = ad.get("name");
    if (type.isEmpty()) {
      throw new RuntimeException("no credential type provided");
    } if (type.equals("gss-cert")) {
      return new StorkGSSCred(ad).name(name);
    } if (type.equals("userinfo")) {
      return new StorkUserinfo(ad).name(name);
    } throw new RuntimeException("invalid credential type: "+type);
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
             .put("timer", time);
  }
}
