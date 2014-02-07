package stork.feather;

import java.lang.ref.*;
import java.util.*;

// A simple object interning utility which uses soft references. The use of
// soft references allowing objects to be kept until the garbage collector
// needs to make some room. This class should only be used for read-only
// objects, as interned objects are shared.

public class Intern<O> {
  private Map<O, SoftReference<O>> map =
    new WeakHashMap<O, SoftReference<O>>();


  // Globally intern a string.
  private static Intern<String> STRING_INTERN = new Intern<String>();
  public static String string(String s) {
    return STRING_INTERN.intern(s);
  }

  public O intern(O k) {
    if (k == null)
      return null;

    SoftReference<O> s = map.get(k);

    if (s == null || s.get() == null) {
      map.put(k, s = new SoftReference<O>(k));
    }

    return s.get();
  }
}
