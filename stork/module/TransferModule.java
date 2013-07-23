package stork.module;

import stork.ad.*;
import stork.util.*;
import stork.scheduler.*;
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

  // Unmarshal transfer modules by name.
  public static TransferModule unmarshal(String s) {
    return byHandle(s);
  }

  // Lookup a transfer module by name.
  public static TransferModule byHandle(String s) {
    return TransferModuleTable.instance().byHandle(s);
  }

  // Lookup a transfer module by protocol.
  public static TransferModule byProtocol(String s) {
    return TransferModuleTable.instance().byProtocol(s);
  }

  // Start a session and perform a transfer.
  // TODO: Remove this.
  public StorkTransfer transfer(SubmitAd ad) {
    StorkSession src = session(ad.src);
    StorkSession dest = session(ad.dest);
    src.pair(dest);
    return new StorkTransfer(src, ad);
  }

  // Start a session and perform a listing.
  public Ad list(URI uri) {
    return list(new EndPoint(uri));
  } public Ad list(EndPoint ep) {
    try {
      return session(ep).list(ep.path(), null);
    } catch (Exception e) {
      throw new FatalEx("couldn't list: "+e.getMessage());
    }
  }

  // Create a new session capable of interacting with a URI.
  public StorkSession session(String uri) {
    return session(new EndPoint(uri));
  } public StorkSession session(URI uri) {
    return session(new EndPoint(uri));
  } public abstract StorkSession session(EndPoint e);

  public String name() {
    return infoAd().full_name;
  }

  public String toString() {
    return infoAd().handle;
  }
}
