package stork.module;

import stork.util.*;
import java.net.URI;

// Abstract base class for a Stork transfer module.

public abstract class TransferModule {
  public abstract ClassAd info_ad();
  public abstract String[] protocols();
  public abstract String name();
  public abstract String version();

  public abstract StorkTransfer transfer(ClassAd ad);

  // Generate a new ClassAd from given URLs.
  public StorkTransfer transfer(String src, String dest) {
    ClassAd ad = new ClassAd();
    ad.insert("src_url", src);
    ad.insert("dest_url", dest);
    return transfer(ad);
  }

  public String toString() {
    if (version() != null)
      return name()+" "+version();
    else
      return name();
  }

  // Static helper method to split protocol strings.
  public static String[] splitProtocols(String s) {
    return (s == null) ? null : s.toLowerCase().split("[,\\s]");
  }
}
