package stork.cred;

import stork.util.*;

// An interface for credentials.

public abstract class StorkCred<O> {
  private String owner;
  private O cred_obj;
  private final Watch timer;

  // Create a new instance of this wrapping a credential.
  public StorkCred(String owner, O cred) {
    this.owner = owner;
    cred_obj = cred;
    timer = new Watch(true);
  }

  // Get the string representation of the credential type.
  public abstract String type();

  // Get/set the id of the user this credential is for.
  public StorkCred owner(String o) {
    owner = o;
    return this;
  } public String owner() {
    return owner;
  }

  // Get/set the object held by this credential wrapper.
  public StorkCred credential(O c) {
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
}
