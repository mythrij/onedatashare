package stork.ad;

import static stork.util.StorkUtil.splitCSV;

import java.util.*;
import java.io.*;
import java.lang.reflect.*;

public class Ad {
  // The heart of the structure.
  protected final Map<Object, AdObject> map;

  private static class AdKey {
    Object v;
    AdKey(String k)  { v = k; }
    AdKey(Integer k) { v = k; }
    public int hashCode() { return v.hashCode(); }
    public boolean equals(Object o) { return v.equals(o); }
  }

  // Create a new ad, plain and simple.
  public Ad() {
    this(true, null);
  }

  // Allows subclasses to take maps from input ads directly to prevent
  // needless copying.
  protected Ad(boolean copy, Ad ad) {
    map = (ad == null) ? new LinkedHashMap<Object, AdObject>() :
          (copy) ? new LinkedHashMap<Object, AdObject>(ad.map) : ad.map;
  }

  // Create a new ad from a list or map.
  public Ad(Iterable<?> list) {
    this(true, null);
    int i = 0;
    for (Object o : list)
      put(o);
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

  // Merges all of the ads passed into this ad.
  public Ad(Ad... bases) {
    this(); merge(bases);
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
    } if (okey instanceof Number) {
      i = ((Number)okey).intValue();
      AdObject o = map.get(i);
      if (o != null)
        return o;
      return getEntry(i).getValue();
    } else {
      String key = okey.toString();
      while ((i = key.indexOf('.')) > 0) synchronized (ad) {
        String k1 = key.substring(0, i);
        AdObject o = ad.map.get(k1);
        if (o == null)
          return null;
        ad = o.asAd();
        key = key.substring(i+1);
      }

      // No more ads to traverse, get value.
      synchronized (ad) {
        return ad.map.get(key);
      }
    }
  }

  // Get an entry from the map.
  private Map.Entry<Object, AdObject> getEntry(int i) {
    if (i < 0 || i >= size())
      throw new IndexOutOfBoundsException();
    for (Map.Entry<Object, AdObject> e : map.entrySet())
      if (i-- == 0) return e;
    return null;
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
    return putObject(null, value);
  } synchronized Ad putObject(Object okey, Object value) {
    int i;
    Ad ad = this;

    if (okey == null) {
      map.put(size(), new AdObject(value));
    } else if (okey instanceof Number) {
      i = ((Number)okey).intValue();
      getEntry(i).setValue(new AdObject(value));
    } else {
      String key = okey.toString();
      // Keep traversing ads until we find the ad we need to insert into.
      while ((i = key.indexOf('.')) > 0) synchronized (ad) {
        String k1 = key.substring(0, i);
        AdObject o = ad.map.get(k1);
        if (o == null)
          ad.map.put(k1, new AdObject(ad = new Ad()));
        else 
          ad = o.asAd();
        key = key.substring(i+1);
      }
      
      // No more ads to traverse, insert object.
      synchronized (ad) {
        if (value != null)
          ad.map.put(key, new AdObject(value));
        else
          ad.map.remove(key);
      }
    } return this;
  }

  // Other methods
  // -------------
  // Methods to get information about and perform operations on the ad.

  public synchronized boolean isEmpty() {
    return map.isEmpty();
  }

  public synchronized void clear() {
    map.clear();
  }

  public synchronized void putAll(Map<String, Object> m) {
    for (Map.Entry<String, Object> e : m.entrySet()) {
      putObject(e.getKey(), e.getValue());
    }
  }

  public synchronized Set<Object> keySet() {
    return map.keySet();
  }

  public synchronized Collection<AdObject> values() {
    return map.values();
  }

  public synchronized Set<Map.Entry<Object, AdObject>> entrySet() {
    return map.entrySet();
  }
  
  // Get the number of fields in this ad.
  public synchronized int size() {
    return map.size();
  }

  // Check if fields or values are present in the ad.
  public synchronized boolean containsKey(Object key) {
    return has(key.toString());
  } public synchronized boolean has(String... keys) {
    return require(keys) == null;
  }

  public synchronized boolean containsValue(Object val) {
    return map.containsValue(val);
  }

  // Ensure that all fields are present in the ad. Returns the first
  // string which doesn't exist in the ad, or null if all are found.
  public synchronized String require(String... keys) {
    for (String k : keys) if (getObject(k) == null)
      return k;
    return null;
  }

  // Merge ads into this one.
  // XXX Possible race condition? Just don't do crazy stuff like try
  // to insert two ads into each other at the same time.
  public synchronized Ad merge(Ad... ads) {
    for (Ad a : ads) if (a != null)
      synchronized (a) { map.putAll(a.map); }
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
    Iterator<Map.Entry<Object, AdObject>> it = map.entrySet().iterator();
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
  private synchronized Object removeKey(String key) {
    int i;
    Ad ad = this;

    // Keep traversing ads until we find ad we need to remove from.
    while ((i = key.indexOf('.')) > 0) synchronized (ad) {
      String k1 = key.substring(0, i);
      Object o = ad.map.get(k1);
      if (o instanceof Ad)
        ad = (Ad) o;
      else return this;
      key = key.substring(i+1);
    }

    // No more ads to traverse, remove key.
    synchronized (ad) {
      return ad.map.remove(key);
    }
  }

  // Two ads are equal if they have the same keys and the corresponding
  // values are equal.
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (o instanceof Ad)
      return ((Ad)o).map.equals(this.map);
    return false;
  }

  public int hashCode() {
    return map.hashCode();
  }

  // Quick hack thing. Check if everything is anonymous.
  public boolean isList() {
    for (Object o : map.keySet())
      if (!(o instanceof Number)) return false;
    return true;
  }

  // Marshalling
  // -----------
  // Methods for serializing and deserializing Java objects as Ads/JSON.

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
