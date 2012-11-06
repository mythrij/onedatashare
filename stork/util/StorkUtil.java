package stork.util;

import java.util.*;
import java.util.regex.*;

// A bunch of static utility functions, how fun!

public class StorkUtil {
  // Some pre-compiled regexes.
  public static final Pattern
    regex_ws   = Pattern.compile("\\s+"),
    regex_csv  = Pattern.compile("\\s*(,\\s*)+"),
    regex_norm = Pattern.compile("[^a-z_0-9\\Q-_+,.\\E]+");

  private StorkUtil() { /* I sure can't be instantiated. */  }

  // String functions
  // ----------------
  // All of these functions should take null and treat it like "".

  // Normalize a string by lowercasing it, replacing spaces with _,
  // and removing characters other than alphanumerics or: -_+.,
  public static String normalize(String s) {
    if (s == null) return "";

    s = s.toLowerCase();
    s = regex_norm.matcher(s).replaceAll(" ").trim();
    s = regex_ws.matcher(s).replaceAll("_");

    return s;
  }

  // Split a CSV string into an array of normalized strings.
  public static String[] splitCSV(String s) {
    String[] a = regex_csv.split(normalize(s), 0);
    return (a == null) ? new String[0] : a;
  }

  // Collapse a string array back into a CSV string.
  public static String joinCSV(String... sa) {
    StringBuffer sb = new StringBuffer();
    if (sa != null && sa.length != 0) {
      sb.append(sa[0]);
      for (int i = 1; i < sa.length; i++)
        sb.append(", "+sa[i]);
    } return sb.toString();
  }
}
