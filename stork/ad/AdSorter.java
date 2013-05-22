package stork.ad;

import java.util.*;

// Sort linked ads by one of their fields.

public class AdSorter {
  // Return an alphabetical comparator.
  public static Comparator<Ad> alphaComparator(final String key) {
    return new Comparator<Ad>() {
      public int compare(Ad a1, Ad a2) {
        return a1.get(key, "").compareTo(a2.get(key, ""));
      } public boolean equals(Object obj) {
        return obj == this;
      }
    };
  }

  // Sort this ad and all of its linked ads.
  public static Ad sort(Ad ads, String key) {
    Ad first = ads, ad = null;
    Set<Ad> set = new TreeSet<Ad>(alphaComparator(key));
    for (Ad a : ads)
      set.add(a);
    for (Ad a : set) if (ad == null)
      first = ad = new Ad(a);
    else
      ad = ad.next(new Ad(a));
    return first;
  }
}
