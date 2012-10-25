package stork.util;

import java.util.*;

// Isn't crazy that Java doesn't have one of these natively?!

public class Range implements Iterable<Integer> {
  private int start, end;
  private Range subrange = null;

  public Range(int i) {
    this(i, i);
  }

  public Range(int s, int e) {
    if (s > e) { int t = s; s = e; e = t; }
    start = s; end = e;
  }

  public Range(String str) {
    this(parseRange(str));
  }

  private Range(Range r) {
    start = r.start;
    end = r.end;
    subrange = r.subrange;
  }

  // Parse a range from a string.
  public static Range parseRange(String str) {
    int s = -1, e = -1;
    Range r = null;

    for (String a : str.split(",")) try {
      String[] b = a.split("-", 3);
      switch (b.length) {
        default: return null;
        case 1: s = e = Integer.parseInt(b[0]); break;
        case 2: s = Integer.parseInt(b[0]);
                e = Integer.parseInt(b[1]);
      } if (s == -1 || e == -1) {
        return null;
      } if (r == null) {
        r = new Range(s, e);
      } else {
        r.swallow(s, e);
      }
    } catch (Exception ugh) {
      // Parse error...
    } return r;
  }

  // Swallow (s,e) into this range so that this range includes
  // all of (s,e) in order and with minimal number of subranges.
  public Range swallow(int s, int e) {
    if (s > e) { int t = s; s = e; e = t; }

    // Eat as much of (s,e) as we can with this range.
    if (s < start) {  // Consider inserting before...
      if (e+1 < start) {  // Independent. Shuffle down ranges...
        Range r = new Range(this);
        start = s; end = e; subrange = r; s = r.end;
      } else {  // Overlapping or adjacent! Easy mode.
        start = s; s = end;
      }
    } if (e > end) {  // Consider inserting after...
      // Let subrange have it, if we have one.
      if (subrange != null)
        subrange.swallow(s, e);
      else
        subrange = new Range(s, e);

      // Now see if we can swallow subrange.
      while (subrange != null && end+1 >= subrange.start) {
        end = subrange.end;
        subrange = subrange.subrange;
      }
    } return this;
  }

  public Range swallow(int i) {
    return swallow(i, i);
  }

  // Swallow a range, subrange by subrange.
  public Range swallow(Range r) {
    while (r != null) {
      swallow(r.start, r.end);
      r = r.subrange;
    } return this;
  }

  // Check if a range contains an integer or range of integers.
  public boolean contains(int i) {
    return contains(i, i);
  }

  public boolean contains(int s, int e) {
    if (s > e) { int t = s; s = e; e = t; }
    return (start <= s && e <= end) ||
           (subrange != null && subrange.contains(s, e));
  }

  // Return a string representation of this range. This string
  // can be parsed back into a range.
  public String toString() {
    return ((start == end) ? ""+start : start+"-"+end) +
           ((subrange != null) ? ","+subrange : "");
  }

  // Get an iterator for this range.
  public Iterator<Integer> iterator() {
    return new Iterator<Integer>() {
      Range r = Range.this;
      int i = r.start;
      private void nextSubrange() {
        if (r == null) return;
        r = r.subrange;
        if (r != null) i = r.start;
      }
      public boolean hasNext() {
        return i <= r.end || r.subrange != null;
      }
      public Integer next() {
        if (r == null)  return null;
        if (i <= r.end) return new Integer(i++);
        nextSubrange(); return next();
      }
      public void remove() { }
    };
  }
}
