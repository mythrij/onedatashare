package stork.cred;

import stork.feather.*;
import stork.scheduler.*;
import stork.util.*;

// An extended Feather credential with additional metadata.

public abstract class StorkCred<O> extends Credential<O> {
  public String type;
  public String name = "(unnamed)";

  public StorkCred(String type) { this.type = type; }

  public static StorkCred newFromType(String type) {
    if (type.equals("userinfo"))
      return new StorkUserinfo();
    if (type.equals("gss"))
      return new StorkGSSCred();
    throw new RuntimeException("Unknown credential type.");
  }

  // Get an info object suitable for showing to users. It should not include
  // sensitive information.
  public Object getInfo() {
    return new Object() {
      String name = StorkCred.this.name;
      String type = StorkCred.this.type;
    };
  }
}
