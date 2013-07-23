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

    public int compare(Ad a1, Ad a2) {
      for (String k : keys) {
        int m = (k.charAt(0) == '-') ? -1 : 1;
        if (m == -1) k = k.substring(1);
        int c = a1.getObject(k).compareTo(a2.getObject(k));
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

  // Add an ad to the ad sorter.
  public void add(Collection<Ad> ads) {
    set.addAll(ads);
  } public void add(Ad... ads) {
    add(Arrays.asList(ads));
  }

  // Get the sorted linked ad from the sorter.
  public List<Ad> getAds() {
    return new LinkedList<Ad>(reverse ? set.descendingSet() : set);
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
