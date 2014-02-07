package stork.cred;

import stork.ad.*;
import java.util.*;

// Maintains a mapping between credential tokens and credentials.
// Automatically handles pruning of expired credentials.

public class CredManager extends LinkedHashMap<UUID, StorkCred<?>> {
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
    StorkCred<?> c = super.get(u);
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
    //put(UUID.fromString(uuid), StorkCred.create(ad));
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

  // Get all the credentials with tokens from the set.
  public Set<Ad> getCredInfo(Collection<String> query) {
    Set<Ad> creds = new HashSet<Ad>();
    for (String s : query)
      creds.add(getCred(s).getAd());
    return creds;
  }
} 
