package stork.cred;

import stork.util.*;

// An interface for credentials.

public abstract class StorkCred<O> {
  private final String user_id;
  private O cred_obj;
  private final Watch timer;

  // Create a new instance of this wrapping a credential.
  public StorkCred(String user, O cred) {
    user_id = user;
    cred_obj = cred;
    timer = new Watch(true);
  }

  // Get the string representation of the credential type.
  public abstract String type();

  // Get the id of the user this credential is for.
  public String userID() {
    return user_id;
  }

  // Get/set the object held by this credential wrapper.
  public O credential(O c) {
    if (c != null)
      cred_obj = c;
    return cred_obj;
  } public O credential() {
    return cred_obj;
  }

  // Get the duration of the credential in milliseconds.
  public long duration() {
    return timer.elapsed();
  }
}
