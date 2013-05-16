package stork.module;

import stork.ad.*;
import stork.util.*;
import java.net.URI;

// Abstract base class for a Stork transfer module.

public abstract class TransferModule {
  public abstract ModuleInfoAd infoAd();

  public abstract Ad validateAd(SubmitAd ad) throws Exception;

  public String handle() {
    return infoAd().handle;
  }

  public String[] protocols() {
    return infoAd().protocols;
  }

  // Start a session and perform a transfer.
  // TODO: Remove this.
  public StorkTransfer transfer(SubmitAd ad) {
    StorkSession src = session(ad.src, ad);
    StorkSession dest = session(ad.dest, ad);
    src.pair(dest);
    return new StorkTransfer(src, ad);
  }

  // Start a session and perform a listing.
  public Ad list(URI uri, Ad opts) {
    try {
      return session(uri, opts).list(uri.getPath(), opts);
    } catch (Exception e) {
      throw new FatalEx("couldn't list: "+e.getMessage());
    }
  }

  // Create a new session capable of interacting with a URI.
  public StorkSession session(String url) throws Exception {
    return session(new URI(url), null);
  } public StorkSession session(String url, Ad opts) throws Exception {
    return session(new URI(url), opts);
  } public StorkSession session(URI url) {
    return session(url, null);
  } public abstract StorkSession session(URI url, Ad opts);

  public String toString() {
    return infoAd().full_name;
  }
}
