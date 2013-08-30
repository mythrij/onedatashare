package stork.cred;

import stork.util.*;
import stork.ad.*;

// An interface for credentials.

public abstract class StorkCred<O> {
  protected String type;
  private String owner;
  private O cred_obj;
  private final Watch timer;

  // Create a new instance of this wrapping a credential.
  public StorkCred(String owner, O cred) {
    this.owner = owner;
    cred_obj = cred;
    timer = new Watch(true);
  }

  // Create a credential from an ad.
  // TODO: This is very hacky and can be done better.
  public static StorkCred<?> create(Ad ad) {
    String type = ad.get("type", "").toLowerCase();
    if (type.isEmpty()) {
      throw new RuntimeException("no credential type provided");
    } if (type.equals("gss_cert")) {
      return new StorkGSSCred(ad);
    } if (type.equals("userinfo")) {
      return new StorkUserinfo(ad);
    } throw new RuntimeException("invalid credential type: "+type);
  }

  // Get the string representation of the credential type.
  public String type() {
    return type;
  }

  // Get/set the id of the user this credential is for.
  public StorkCred<?> owner(String o) {
    owner = o;
    return this;
  } public String owner() {
    return owner;
  }

  // Get/set the object held by this credential wrapper.
  public StorkCred<?> credential(O c) {
    if (c != null)
      cred_obj = c;
    return this;
  } public O credential() {
    return cred_obj;
  }

  // Get the duration of the credential in milliseconds.
  public long duration() {
    return timer.elapsed();
  }

  // Get an ad suitable for showing to users. It should not include
  // sensitive information.
  public Ad getAd() {
    return new Ad("type", type)
             .put("owner", owner)
             .put("timer", timer);
  }
}
