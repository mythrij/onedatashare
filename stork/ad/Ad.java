package stork.ad;

import static stork.util.StorkUtil.splitCSV;

import java.util.*;
import java.io.*;
import java.lang.ref.*;
import java.lang.reflect.*;

public class Ad implements Serializable {
  // There's some kind of irony here, isn't there? :)
  static final long serialVersionUID = 5988172454007663702L;

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

  // Ad keys are interned using weak references.
  private static Map<String, SoftReference<String>> internMap =
    new WeakHashMap<String, SoftReference<String>>();
  public static String intern(String k) {
    SoftReference<String> s = internMap.get(k);
    if (s == null)
      internMap.put(k, s = new SoftReference<String>(k));
    return s.get();
  }

  // Create a new ad, plain and simple.
  public Ad() { }

  // Create a new ad from an array.
  public Ad(Object[] list) {
    addAll(Arrays.asList(list));
  }

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
  } public Ad(String key, List<?> value) {
    this(); put(key, value);
  }

  // Static parser methods. These will throw runtime exceptions if there
  // is a parse error, and will return null if EOF is encountered
  // prematurely.
  public static Ad parse(CharSequence cs) {
    return parse(cs, false);
  } public static Ad parse(InputStream is) {
    return parse(is, false);
  } public static Ad parse(File f) {
    return parse(f, false);
  }

  public static Ad parse(CharSequence cs, boolean body_only) {
    return new AdParser(cs, body_only).parse();
  } public static Ad parse(InputStream is, boolean body_only) {
    return new AdParser(is, body_only).parse();
  } public static Ad parse(File f, boolean body_only) {
    Reader r = null;
    try {
      return new AdParser(r = new FileReader(f), body_only).parse();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (r != null) try {
        r.close();
      } catch (Exception e) {
        // Ugh, whatever...
      }
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
  } public Ad[] getAds() {
    return getAll(Ad.class);
  }

  // Get a value as a list of a given type. If the value is not an ad,
  // return a list containing just that value. If the key does not
  // exist, returns null.
  public AdObject[] getAll(Object s) {
    return getAll(null, s);
  } public <C> C[] getAll(Class<C> c) {
    return AdObject.wrap(this).asArray(c);
  } public <C> C[] getAll(Class<C> c, Object s) {
    AdObject o = getObject(s);
    return (o != null) ? o.asArray(c) : null;
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
    } if (isList()) {
      if (okey instanceof Integer) {
        AdObject o = list().get((Integer)okey);
        return o;
      } else {
        // Don't choke on an access, just pretend it's not there.
        return null;
      }
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
  public Ad put(Object k, Object... v) {
    switch (v.length) {
      case 0 : return putObject(k);
      case 1 : return putObject(k, v[0]);
      default: return putObject(k, new Ad(v));
    }
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
          ad.map().put(intern(k1), AdObject.wrap(ad = new Ad()));
        else 
          ad = o.asAd();
        key = key.substring(i+1);
      }
      
      // No more ads to traverse, insert object.
      synchronized (ad) {
        if (value != null)
          ad.map().put(intern(key), AdObject.wrap(value));
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

  // Unmarshal this ad into an object. This operation can throw a runtime
  // exception. Should we reset fields if there's an exception?
  public synchronized <O> O unmarshal(O o) {
    if (o.getClass() == Ad.class) {
      ((Ad)o).addAll(this);
    } else {
      for (Field f : fieldsFrom(o))
        marshalField(f, o, false);
    } return o;
  }

  // Construct a new instance of a class and marshal into it.
  public <O> O unmarshalAs(Class<O> clazz, Object... args) {
    Class<?>[] ca = new Class<?>[args.length];
    for (int i = 0; i < args.length; i++) {
      // Hmm, what to do about null arguments...
      ca[i] = (args[i] != null) ? args[i].getClass() : Object.class;
    } try {
      Constructor<O> c = clazz.getConstructor(ca);
      return unmarshal(c.newInstance(args));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Marshal an object into an ad.
  public static Ad marshal(Object o) {
    try {
      if (o instanceof Ad) {
        return (Ad)o;
      } else if (o instanceof Map) {
        return new Ad((Map) o);
      } else if (o instanceof Collection) {
        return new Ad((Collection) o);
      } else if (o instanceof Iterable) {
        return new Ad((Iterable) o);
      } else if (o.getClass().isArray()) {
        return new Ad((Object[])o);
      } else {
        Ad ad = new Ad();
        for (Field f : fieldsFrom(o))
          ad.marshalField(f, o, true);
        return ad;
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Helper method to marshal/unmarshal a field to/from an ad.
  private synchronized void marshalField(Field f, Object o, boolean m) {
    // Ignore certain types of fields.
    int mod = f.getModifiers();
    if (f.isSynthetic() ||
        Modifier.isTransient(mod) ||
        Modifier.isStatic(mod)) return;

    // Skip if unmarshalling and ad has no such member.
    AdObject ao = null;
    if (!m && (ao = getObject(f.getName())) == null)
      return;

    // Make the field accessible.
    boolean accessible = f.isAccessible();
    f.setAccessible(true);

    // Do the deed.
    try {
      if (m) {  // If we're marshalling...
        putObject(f.getName(), f.get(o));
      } else {  // Else unmarshalling (ao is not null).
        f.set(o, ao.as(f.getType()));
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      // Reset access permissions.
      f.setAccessible(accessible);
    }
  }

  // Helper method to get all interesting fields in a class.
  private static Set<Field> fieldsFrom(Object o) {
    return fieldsFrom(o.getClass());
  } private static Set<Field> fieldsFrom(Class<?> c) {
    Set<Field> fields = new HashSet<Field>();
    fields.addAll(Arrays.asList(c.getDeclaredFields()));
    fields.addAll(Arrays.asList(c.getFields()));
    return fields;
  }

  // Composition methods
  // ------------------
  // Obvious an ad as an ad it just itself.
  public Ad toAd() {
    return this;
  }

  // Represent this ad in some kind of nice format. I guess the ClassAd
  // format is easier on the eyes, so let's use that.
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

  // Testing method.
  public static void main(String args[]) {
    System.out.println("Type an ad:");
    Ad ad = Ad.parse(System.in);
    System.out.println("Got ad: \n"+ad);
  }
}
