package stork.cred;

import stork.ad.*;
import java.util.*;

// Maintains a mapping between credential tokens and credentials.
// Automatically handles pruning of expired credentials.

public class CredManager {
  private static CredManager instance = null;

  private Map<UUID, StorkCred> cred_map;

  public CredManager() {
    cred_map = new LinkedHashMap<UUID, StorkCred>();
  }

  // Get an instance of the credential manager.
  public static synchronized CredManager instance() {
    if (instance == null)
      instance = new CredManager();
    return instance;
  }

  // Get a credential from the credential map given a token.
  public synchronized StorkCred getCred(String token) {
    try {
      return getCred(UUID.fromString(token));
    } catch (Exception e) {
      return null;
    }
  } public synchronized StorkCred getCred(UUID token) {
    return (token == null) ? null : cred_map.get(token);
  }

  // Get all the credentials as ads.
  public synchronized List<Ad> allCreds() {
    AdSorter sorter = new AdSorter("uuid");
    for (UUID u : cred_map.keySet()) {
      sorter.add(
        new Ad("uuid", u.toString())
          .put("type", cred_map.get(u).type())
      );
    } return sorter.getAds();
  }

  // Put a credential into the map and return an automatically
  // generated token for the credential.
  public synchronized String putCred(StorkCred cred) {
    UUID uuid;
    do {  // Better safe than sorry. :)
      uuid = UUID.randomUUID();
    } while (cred_map.containsKey(uuid));
    cred_map.put(uuid, cred);
    return uuid.toString();
  }
} 
