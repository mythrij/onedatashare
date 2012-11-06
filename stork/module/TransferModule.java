package stork.module;

import stork.util.*;
import java.net.URI;

// Abstract base class for a Stork transfer module.

public abstract class TransferModule {
  public abstract ModuleInfoAd infoAd();

  public abstract ClassAd validateAd(SubmitAd ad) throws Exception;

  public String handle() {
    return infoAd().handle;
  }

  public String[] protocols() {
    return infoAd().protocols;
  }

  public abstract StorkTransfer transfer(SubmitAd ad);

  // Generate a new ClassAd from given URLs and use that.
  public StorkTransfer transfer(String src, String dest) throws Exception {
    return transfer(new SubmitAd(src, dest));
  }

  public String toString() {
    return infoAd().full_name;
  }
}
