package stork.cred;

import stork.ad.*;
import java.util.*;

// Maintains a mapping between credential tokens and credentials.
// Automatically handles pruning of expired credentials.

public class CredManager extends LinkedHashMap<UUID, StorkCred<?>> {
  static final long serialVersionUID = 5699292025585765417L;

  // Get a credential from the credential map given a token.
  public synchronized StorkCred<?> getCred(String token) {
    try {
      return getCred(UUID.fromString(token));
    } catch (Exception e) {
      return null;
    }
  } public synchronized StorkCred<?> getCred(UUID token) {
    return (token == null) ? null : get(token);
  }

  public StorkCred<?> get(UUID u) {
    System.out.println("Getting: "+u);
    StorkCred<?> c = super.get(u);
    if (c == null)
      System.out.println(u+" wasn't found!");
    System.out.println(keySet());
    return c;
  }

  // TODO: Remove me when ads support this.
  public static CredManager unmarshal(Ad ad) {
    CredManager cm = new CredManager();

    for (String uuid : ad.keySet())
      cm.putFromAd(uuid, ad.getAd(uuid));
    return cm;
  }

  private void putFromAd(String uuid, Ad ad) {
    put(UUID.fromString(uuid), StorkCred.create(ad));
  }

  // Get all the credentials as ads, optionally for a given user.
  public Ad getCredInfos() {
    return getCredInfos(null);
  } public Ad getCredInfos(String user) {
    Ad ad = new Ad();
    for (Map.Entry<UUID, StorkCred<?>> e : entrySet()) {
      StorkCred<?> c = e.getValue();
      if (user == null || user.equals(c.owner()))
        ad.put(e.getKey(), c.getAd());
    } return ad;
  }

  // Put a credential into the map and return an automatically
  // generated token for the credential.
  public synchronized String add(StorkCred<?> cred) {
    UUID uuid;
    do {  // Better safe than sorry. :)
      uuid = UUID.randomUUID();
    } while (containsKey(uuid));
    put(uuid, cred);
    return uuid.toString();
  }
} 
