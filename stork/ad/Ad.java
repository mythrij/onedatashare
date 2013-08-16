package stork.ad;

import static stork.util.StorkUtil.splitCSV;

import java.util.*;
import java.io.*;
import java.lang.reflect.*;

public class Ad {
  // An ad is either a list or a map, but never both. Never access these
  // directly, always access through list() or map().
  private Map<Object, AdObject> map = null;
  private List<AdObject> list = null;

  // Make this ad a map if it's undetermined. Return the map.
  Map<Object, AdObject> map() {
    return map(true);
  } Map<Object, AdObject> map(boolean make) {
    if (list != null)
      throw new RuntimeException("cannot access ad as a map");
    if (map == null && make)
      map = new LinkedHashMap<Object, AdObject>();
    return map;
  }

  // Make this ad a list if it's undetermined. Return the list.
  List<AdObject> list() {
    return list(true);
  } List<AdObject> list(boolean make) {
    if (map != null)
      throw new RuntimeException("cannot access ad as a list");
    if (list == null && make)
      list = new LinkedList<AdObject>();
    return list;
  }

  // Create a new ad, plain and simple.
  public Ad() { }

  // Create a new ad from a list.
  public Ad(Collection<?> list) {
    addAll(list);
  }

  // Create a new ad from an iterable.
  public Ad(Iterable<?> iter) {
    addAll(iter);
  }

  // Create a new ad from a map.
  public Ad(Map<?,?> map) {
    addAll(map);
  }

  // Create a new ad that is the copy of another ad.
  public Ad(Ad ad) {
    addAll(ad);
  }

  // Create an ad with given key and value.
  public Ad(String key, int value) {
    this(); put(key, value);
  } public Ad(String key, long value) {
    this(); put(key, value);
  } public Ad(String key, double value) {
    this(); put(key, value);
  } public Ad(String key, boolean value) {
    this(); put(key, value);
  } public Ad(String key, Number value) {
    this(); put(key, value);
  } public Ad(String key, String value) {
    this(); put(key, value);
  } public Ad(String key, Ad value) {
    this(); put(key, value);
  } public Ad(String key, List value) {
    this(); put(key, value);
  }

  // Static parser methods. These will throw runtime exceptions if there
  // is a parse error, and will return null if EOF is encountered
  // prematurely.
  public static Ad parse(CharSequence cs) {
    return new AdParser(cs).parse();
  } public static Ad parse(InputStream is) {
    return new AdParser(is).parse();
  } public static Ad parse(File f) {
    try {
      return new AdParser(f).parse();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Ad parseBody(CharSequence cs) {
    return new AdParser(cs, true).parse();
  } public static Ad parseBody(InputStream is) {
    return new AdParser(is, true).parse();
  } public static Ad parseBody(File f) {
    try {
      return new AdParser(f, true).parse();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Access methods
  // --------------
  // Each accessor can optionally have a default value passed as a second
  // argument to be returned if no entry with the given name exists.
  // All of these eventually synchronize on getObject.

  // Get an entry from the ad as a string. Default: null
  public String get(Object s) {
    return get(s, null);
  } public String get(Object s, String def) {
    AdObject o = getObject(s);
    return (o != null) ? o.asString() : def;
  }

  // Get an entry from the ad as an integer. Defaults to -1.
  public int getInt(Object s) {
    return getInt(s, -1);
  } public int getInt(Object s, int def) {
    AdObject o = getObject(s);
    return (o != null) ? o.asInt() : def;
  }

  // Get an entry from the ad as a long. Defaults to -1.
  public long getLong(Object s) {
    return getLong(s, -1);
  } public long getLong(Object s, long def) {
    AdObject o = getObject(s);
    return (o != null) ? o.asLong() : def;
  }

  // Get an entry from the ad as a double. Defaults to -1.
  public double getDouble(Object s) {
    return getDouble(s, -1);
  } public double getDouble(Object s, double def) {
    AdObject o = getObject(s);
    return (o != null) ? o.asDouble() : def;
  }

  // Get an entry from the ad as a Number object. Attempts to cast to a
  // number object if it's a string. Defaults to null.
  public Number getNumber(Object s) {
    return getNumber(s, null);
  } public Number getNumber(Object s, Number def) {
    AdObject o = getObject(s);
    return (o != null) ? o.asNumber() : def;
  }

  // Get an entry from the ad as a boolean. Returns true if the value is
  // a true boolean, a string equal to "true", or a number other than zero.
  // Returns def if key is an ad or is undefined. Defaults to false.
  public boolean getBoolean(Object s) {
    return getBoolean(s, false);
  } public boolean getBoolean(Object s, boolean def) {
    AdObject o = getObject(s);
    return (o != null) ? o.asBooleanValue() : def;
  }

  // Get an inner ad from this ad. Defaults to null.
  public Ad getAd(Object s) {
    return getAd(s, null);
  } public Ad getAd(Object s, Ad def) {
    AdObject o = getObject(s);
    return (s != null) ? o.asAd() : def;
  } public Ad[] getAds(Object s) {
    return getAll(Ad.class, s);
  }

  // Get a value as a list of a given type. If the value is not an ad,
  // return a list containing just that value. If the key does not
  // exist, returns null.
  public AdObject[] getAll(Object s) {
    return getAll(null, s);
  } public <C> C[] getAll(Class<C> c, Object s) {
    AdObject o = getObject(s);
    return (o != null) ? o.asList(c) : null;
  }
    
  // Look up an object by its key. Handles recursive ad lookups.
  synchronized AdObject getObject(Object okey) {
    int i;
    Ad ad = this;

    if (okey == null) {
      throw new RuntimeException("null key given");
    } if (isEmpty()) {
      // This is so we don't determine the ad type just because of a get.
      return null;
    } if (okey instanceof Integer && isList()) {
      AdObject o = list().get((Integer)okey);
      return o;
    } else {
      String key = okey.toString();
      while ((i = key.indexOf('.')) > 0) synchronized (ad) {
        String k1 = key.substring(0, i);
        AdObject o = ad.map().get(k1);
        if (o == null)
          return null;
        ad = o.asAd();
        key = key.substring(i+1);
      }

      // No more ads to traverse, get value.
      synchronized (ad) {
        return ad.map().get(key);
      }
    }
  }

  // Insertion methods
  // -----------------
  // Methods for putting values into an ad. Certain primitive types can
  // be stored as their wrapped equivalents to save space, since they are
  // still printed in a way that is compatible with the language.
  // All of these eventually synchronize on putObject.
  public Ad put(Object key, int value) {
    return putObject(key, Integer.valueOf(value));
  }

  public Ad put(Object key, long value) {
    return putObject(key, Long.valueOf(value));
  }

  public Ad put(Object key, double value) {
    return putObject(key, Double.valueOf(value));
  }

  public Ad put(Object key, Boolean value) {
    return putObject(key, value);
  }

  public Ad put(Object key, boolean value) {
    return putObject(key, Boolean.valueOf(value));
  }

  public Ad put(Object key, Object value) {
    return putObject(key, value);
  }

  // If you create a loop with this and print it, I feel bad for you. :)
  public Ad put(Object key, Ad value) {
    return putObject(key, value);
  }

  public Ad put(Object key, List value) {
    return putObject(key, new Ad(value));
  }

  public Ad put(Object value) {
    return putObject(null, value);
  }

  // Use this to insert objects in the above methods. This takes care
  // of validating the key so accidental badness doesn't occur.
  synchronized Ad putObject(Object value) {
    if (value instanceof Map.Entry<?,?>) {
      Map.Entry<?,?> e = (Map.Entry<?,?>) value;
      return putObject(e.getKey(), e.getValue());
    } return putObject(null, value);
  } synchronized Ad putObject(Object okey, Object value) {
    int i;
    Ad ad = this;

    if (okey == null) {
      list().add(AdObject.wrap(value));
    } else {
      String key = okey.toString();
      // Keep traversing ads until we find the ad we need to insert into.
      while ((i = key.indexOf('.')) > 0) synchronized (ad) {
        String k1 = key.substring(0, i);
        AdObject o = ad.map().get(k1);
        if (o == null)
          ad.map().put(k1, AdObject.wrap(ad = new Ad()));
        else 
          ad = o.asAd();
        key = key.substring(i+1);
      }
      
      // No more ads to traverse, insert object.
      synchronized (ad) {
        if (value != null)
          ad.map().put(key, AdObject.wrap(value));
        else
          ad.map().remove(key);
      }
    } return this;
  }

  // Other methods
  // -------------
  // Methods to get information about and perform operations on the ad.

  public synchronized boolean isEmpty() {
    return size() == 0;
  }

  public synchronized void clear() {
    if (isList())
      list().clear();
    else if (isMap())
      map().clear();
  }

  // Clear the ad and make its type undetermined.
  public synchronized void reset() {
    list = null;
    map  = null;
  }

  public synchronized void putAll(Map<String, Object> m) {
    for (Map.Entry<String, Object> e : m.entrySet()) {
      putObject(e.getKey(), e.getValue());
    }
  }

  // Get the number of fields in this ad.
  public synchronized int size() {
    return isMap()  ?  map().size() :
           isList() ? list().size() : 0;
  }

  // Check if fields or values are present in the ad.
  public synchronized boolean containsKey(Object key) {
    return has(key.toString());
  } public synchronized boolean has(String... keys) {
    return require(keys) == null;
  }

  public synchronized boolean containsValue(Object val) {
    return isMap()  ?  map().containsValue(val) :
           isList() ? list().contains(val) : false;
  }

  // Ensure that all fields are present in the ad. Returns the first
  // string which doesn't exist in the ad, or null if all are found.
  public synchronized String require(String... keys) {
    for (String k : keys) if (getObject(k) == null)
      return k;
    return null;
  }

  // Merge this ad and others into a new ad.
  public synchronized Ad merge(Ad... ads) {
    return new Ad(this).addAll(ads);
  }

  // Add all the entries from another ad.
  public synchronized Ad addAll(Ad... ads) {
    for (Ad ad : ads) {
      if (ad == null || ad.isEmpty())
        continue;
      if (ad.isMap())
        return addAll(ad.map());
      else if (ad.isList())
        return addAll(ad.list());
    } return this;
  }

  // Add all the entries from a map.
  public synchronized Ad addAll(Map<?,?> map) {
    return addAll(map.entrySet());
  }

  // Add all the entries from a list.
  public synchronized Ad addAll(Iterable<?> c) {
    for (Object o : c) putObject(o);
    return this;
  }

  // Remove fields from this ad.
  public synchronized Ad remove(String... k) {
    for (String s : k)
      removeKey(s);
    return this;
  }

  // Return a new ad containing only the specified keys.
  // How should this handle sub ads?
  public synchronized Ad filter(String... keys) {
    Ad a = new Ad();
    for (Object k : keys)
      a.putObject(k, getObject(k));
    return a;
  }

  // Rename a key in the ad. Does nothing if the key doesn't exist.
  public synchronized Ad rename(String o, String n) {
    Object obj = removeKey(o);
    if (obj != null)
      putObject(n, obj);
    return this;
  }

  // Trim strings in this ad, removing empty strings.
  public synchronized Ad trim() {
    if (isEmpty())
      return this;
    if (isList())
      return trimList();
    if (isMap())
      return trimMap();
    return this;
  } private synchronized Ad trimList() {
    Iterator<AdObject> it = list().iterator();
    while (it.hasNext()) {
      AdObject o = it.next();

      if (o.asObject() instanceof String) {
        String s = o.toString().trim();
        if (s.isEmpty())
          it.remove();
        else o.setObject(s);
      } else if (o.asObject() instanceof Ad) {
        o.asAd().trim();
      }
    } return this;
  } private synchronized Ad trimMap() {
    Iterator<Map.Entry<Object, AdObject>> it = map().entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<Object, AdObject> e = it.next();
      AdObject o = e.getValue();

      if (o.asObject() instanceof String) {
        String s = o.toString().trim();
        if (s.isEmpty())
          it.remove();
        else o.setObject(s);
      } else if (o.asObject() instanceof Ad) {
        o.asAd().trim();
      }
    } return this;
  }

  // Remove a key from the ad, returning the old value (or null).
  private synchronized Object removeKey(Object okey) {
    int i;
    Ad ad = this;

    // Quick and easy check.
    if (isEmpty())
      return null;

    // See if we're removing an index from the list.
    if (okey instanceof Integer && isList())
      return list().remove((Integer)okey);

    String key = okey.toString();

    // Keep traversing ads until we find ad we need to remove from.
    while ((i = key.indexOf('.')) > 0) synchronized (ad) {
      String k1 = key.substring(0, i);
      Object o = ad.map().get(k1);
      if (o instanceof Ad)
        ad = (Ad) o;
      else return this;
      key = key.substring(i+1);
    }

    // No more ads to traverse, remove key.
    synchronized (ad) {
      return ad.map().remove(key);
    }
  }

  // Two ads are equal if they have the same keys and the corresponding
  // values are equal.
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    } if (o instanceof Ad) {
      Ad ad = (Ad)o;
      if (isList() && ad.isList())
        return list().equals(ad.list());
      else if (isMap() && ad.isMap())
        return map().equals(ad.map());
      return isEmpty() && ad.isEmpty();
    } else {
      return false;
    }
  }

  public int hashCode() {
    return isList() ? list().hashCode() :
           isMap()  ?  map().hashCode() : 0;
  }

  public boolean isList() {
    return list != null;
  } public boolean isMap() {
    return map != null;
  } public boolean isUndetermined() {
    return list == map;  // :)
  }

  // Marshalling
  // -----------
  // Methods for marshalling objects to/from ads.

  // Unmarshal an object from this ad.
  public <T> T unmarshalAs(Class<T> c) {
    return unmarshalAs(c, null);
  } public <T> T unmarshalAs(Class<T> c, Object key) {
    Object o = (key != null) ? getObject(key).asObject() : this;
    T t = null;  // The object we return.

    if (o == null) {
      // If there's no object with the given key, return null.
      return null;
    } if (c.isInstance(o)) {
      // If it's already the desired class, just return it.
      return c.cast(o);
    } try {
      // If it's declared unmarshallable, look for an unmarshal method.
      try {
        // Look for a unmarshalling method which takes the stored type.
        t = c.cast(c.getMethod("unmarshal", o.getClass()).invoke(null, o));
      } catch (NoSuchMethodException e) {
        // Look for a unmarshalling method that takes an object.
        t = c.cast(c.getMethod("unmarshal", Object.class).invoke(null, o));
      }
    } catch (NoSuchMethodException e) {
      /* fall through */
    } catch (RuntimeException e) {
      // Something bad happened while unmarshalling.
      throw e;
    } catch (Exception e) {
      // Something bad happened while unmarshalling. Wrap it.
      throw new RuntimeException(e);
    } if (o instanceof Ad && t == null) {
      // If the stored object is an ad, try unmarshalling the ad.
      t = ((Ad)o).unmarshal(c);
    }

    return t;
  }

  // Unmarshal this ad into an object. This operation can throw a runtime
  // exception.
  public synchronized <O> O unmarshal(O o) {
    try {
      if (o.getClass() == Ad.class) {
        ((Ad)o).merge(this);
        return o;
      } else for (Field f : o.getClass().getFields()) synchronized (f) {
        // Iterate over class fields and see if there's anything in the ad.
        // Ignore certain types of fields.
        int m = f.getModifiers();
        if (f.isSynthetic() ||
            Modifier.isTransient(m) ||
            Modifier.isStatic(m)) continue;

        Class<?> c = f.getType();
        AdObject a = getObject(f.getName());

        // Ignore nulls.
        if (a == null) continue;

        // Make the field accessible for our purposes.
        boolean accessible = f.isAccessible();
        f.setAccessible(true);

        if (a.isAd())
          f.set(o, unmarshal(c));
        else
          f.set(o, a.as(c));

        // Replace original access permissions.
        f.setAccessible(accessible);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } return o;
  }

  // For objects to be used with this, they must implement a zero-arg
  // constructor.
  public <O> O unmarshal(Class<O> clazz) {
    try {
      O o = clazz.newInstance();
      unmarshal(o);
      return o;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Marshal an object into this ad.
  public static Ad marshal(Object o) {
    try {
      if (o instanceof Ad) {
        return (Ad)o;
      } else if (o instanceof Map) {
        return fromMap((Map) o);
      } else if (o instanceof Collection) {
        return fromList((Collection) o);
      } else if (o.getClass().isArray()) {
        return fromArray(o);
      } else {
        return fromObject(o);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Ad fromMap(Map<?,?> m) {
    Ad ad = new Ad();
    for (Map.Entry<?,?> e : m.entrySet())
      ad.putObject(e.getKey(), e.getValue());
    return ad;
  } private static Ad fromList(Collection<?> l) {
    Ad ad = new Ad();
    for (Object o : l) ad.putObject(o);
    return ad;
  } private static Ad fromArray(Object o) {
    Ad ad = new Ad();
    for (int i = 0; i < Array.getLength(o); i++)
      ad.putObject(Array.get(o, i));
    return ad;
  } private static Ad fromObject(Object o) {
    Ad ad = new Ad();
    for (Field f : o.getClass().getFields()) try {
      if (f.get(o) != null)
        ad.putObject(f.getName(), f.get(o));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } return ad;
  }

  // Composition methods
  // ------------------
  // Represent this ad in some kind of nice format. I guess the ClassAd
  // format is easier on the eyes.
  public synchronized String toString(boolean pretty) {
    return toClassAd(pretty);
  } public synchronized String toString() {
    return toClassAd(true);
  }

  // Represent this ad as a JSON string.
  public synchronized String toJSON(boolean pretty) {
    return (pretty ? AdPrinter.JSON : AdPrinter.JSON_MIN).toString(this);
  } public synchronized String toJSON() {
    return toJSON(true);
  }

  // Represent this ad as a ClassAd string.
  public synchronized String toClassAd(boolean pretty) {
    return (pretty ? AdPrinter.CLASSAD : AdPrinter.CLASSAD_MIN).toString(this);
  } public synchronized String toClassAd() {
    return toClassAd(true);
  }

  public static void main(String args[]) {
    System.out.println("Type an ad:");
    Ad ad = Ad.parse(System.in);
    System.out.println("Got ad: \n"+ad);
  }
}
