package stork.ad;

import java.util.*;

// Sort ads by their fields. Prepending a minus sign to the beginning of
// a key name causes that key to be sorted in reverse.

public class AdSorter {
  TreeSet<Ad> set;
  Comparator<Ad> comparator;
  boolean reverse = false;

  // Create an ad sorter to sort on the given key.
  public AdSorter(String... keys) {
    set = new TreeSet<Ad>(comparator = new AdComparator(keys));
  } public AdSorter(Comparator<Ad> ac) {
    set = new TreeSet<Ad>(comparator = ac);
  }

  // Comparator for comparing ad fields. Supports additional features,
  // like reversing the comparator.
  public static class AdComparator implements Comparator<Ad> {
    private String[] keys;

    // Construct a comparator to sort on the given key or keys.
    public AdComparator(String... keys) {
      this.keys = keys;
    }

    private static int compareObjects(Object o1, Object o2) {
      if (o1 == o2)   return  0;
      if (o1 == null) return -1;
      if (o2 == null) return  1;
      if (o1 instanceof String && o2 instanceof String)
        return ((String)o1).compareTo((String)o2);
      if (o1 instanceof Number && o2 instanceof Number)
        return (int) Math.signum(
          ((Number)o1).doubleValue()-((Number)o2).doubleValue());
      if (o1 instanceof Boolean && o2 instanceof Boolean)
        return ((Boolean)o1).compareTo((Boolean)o2);
      return o1.hashCode() - o2.hashCode();  // FIXME: No no no!
    }

    public int compare(Ad a1, Ad a2) {
      for (String k : keys) {
        int m = (k.charAt(0) == '-') ? -1 : 1;
        if (m == -1) k = k.substring(1);
        int c = compareObjects(a1.getObject(k), a2.getObject(k));
        if (c != 0) return c*m;
      } return 0;
    }
  }

  // Set/toggle whether or not to reverse the sort order.
  public boolean reverse() {
    return reverse = !reverse;
  } public boolean reverse(boolean rev) {
    return reverse = rev;
  }

  // Sort this ad and all of its linked ads.
  public static Ad sort(Ad ads, String key) {
    AdSorter sorter = new AdSorter(key);
    for (Ad a : ads)
      sorter.add(a);
    return sorter.getAd();
  }

  // Add an ad or a list of ads to the ad sorter.
  public void add(Ad[] ads) {
    add(Arrays.asList(ads));
  } public void add(Collection<? extends Ad> ads) {
    set.addAll(ads);
  } public void add(Ad ad) {
    set.add(ad);
  }

  // Get the sorted linked ad from the sorter.
  public Ad getAd() {
    Ad first = null, ad = null;
    Set<Ad> s = reverse ? set.descendingSet() : set;
    for (Ad a : s) if (ad == null)
      first = ad = new Ad(false, a);
    else
      ad = ad.next(new Ad(false, a));
    return first;
  }

  // Get the number of ads in the sorter.
  public int size() {
    return set.size();
  }

  // Empty the sorter.
  public void clear() {
    set.clear();
  }
}
