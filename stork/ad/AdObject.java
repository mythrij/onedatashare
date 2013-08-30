package stork.ad;

import java.util.*;
import java.math.*;
import java.lang.reflect.*;

// Beware all ye who enter, for here there be dragons.

public class AdObject implements Comparable<AdObject> {
  Object object;

  // A map of types to methods for converting to that type.
  private static Map<Class<?>, Method> conv_map = 
    new HashMap<Class<?>, Method>();

  // Add a method to the conversion map.
  private static void map(Class<?> c, String m) {
    try {
      conv_map.put(c, AdObject.class.getMethod(m));
    } catch (Exception e) {
      // Ignore it.
    }
  }

  static {
    map(null,          "asObject");
    map(Object.class,  "asObject");
    map(Ad.class,      "asAd");
    map(String.class,  "asString");
    map(Number.class,  "asNumber");
    map(Integer.class, "asInt");
    map(Double.class,  "asDouble");
    map(Float.class,   "asFloat");
    map(Byte.class,    "asByte");
    map(Long.class,    "asLong");
    map(Short.class,   "asShort");
    map(Boolean.class, "asBoolean");
    map(Character.class, "asChar");
    map(int.class,     "asInt");
    map(double.class,  "asDouble");
    map(float.class,   "asFloat");
    map(byte.class,    "asByte");
    map(long.class,    "asLong");
    map(short.class,   "asShort");
    map(boolean.class, "asBooleanValue");
    map(char.class,    "asChar");
    map(Map.class,     "asMap");
    map(Collection.class, "asList");
    map(java.net.URI.class, "asURI");
  }

  private AdObject(Object o) {
    object = marshal(o);
  }

  // Convert an object to an ad primitive type.
  // FIXME: Aaaa isn't there a better way?
  private static Object marshal(Object o) {
    if (o == null)
      return o;
    if (o instanceof String)
      return o;
    if (o instanceof Number)
      return o;
    if (o instanceof Boolean)
      return o;
    if (o instanceof java.net.URI)
      return o.toString();
    if (o instanceof Character)
      return o.toString();
    if (o instanceof Enum)
      return o.toString();
    if (o instanceof Collection)
      return new Ad((Collection) o);
    if (o instanceof Iterable)
      return new Ad((Iterable) o);
    if (o instanceof Map)
      return new Ad((Map) o);
    if (o.getClass().isArray())
      return new Ad((Object[]) o);
    return Ad.marshal(o);
  }

  public static AdObject wrap(Object o) {
    return (o instanceof AdObject) ? (AdObject) o : new AdObject(o);
  }

  public AdObject setObject(Object o) {
    object = o;
    return this;
  }

  public boolean isAd() {
    return object instanceof Ad;
  }

  public boolean isString() {
    return object instanceof String;
  }

  public Object asObject() {
    return object;
  }

  public String asString() {
    return (object != null) ? object.toString() : null;
  } public String toString() {
    return (object != null) ? object.toString() : null;
  }

  public java.net.URI asURI() {
    return java.net.URI.create(asString());
  }

  public Number asNumber() {
    if (object instanceof Number)
      return (Number) object;
    if (object instanceof String)
      return new BigDecimal(object.toString());
    if (object instanceof Boolean)
      return Integer.valueOf(((Boolean)object).booleanValue() ? 1 : 0);
    throw new RuntimeException("cannot convert to number");
  }

  // Primitive numbers.
  public int asInt() {
    return asNumber().intValue();
  } public double asDouble() {
    return asNumber().doubleValue();
  } public float asFloat() {
    return asNumber().floatValue();
  } public byte asByte() {
    return asNumber().byteValue();
  } public long asLong() {
    return asNumber().longValue();
  } public short asShort() {
    return asNumber().shortValue();
  } public char asChar() {
    return asString().charAt(0);
  }

  public Boolean asBoolean() {
    if (object instanceof Boolean)
      return ((Boolean)object);
    if (object instanceof String)
      return Boolean.valueOf(toString().equalsIgnoreCase("true"));
    if (object instanceof Number)
      return Boolean.valueOf(((Number)object).intValue() != 0);
    throw new RuntimeException("cannot convert to boolean from "+object.getClass());
  } public boolean asBooleanValue() {
    return asBoolean().booleanValue();
  }

  public Ad asAd() {
    if (object instanceof Ad)
      return (Ad)object;
    throw new RuntimeException("cannot convert "+object.getClass()+" to ad");
  }

  public Collection<?> asList() {
    return asAd().intoCollection(new LinkedList<Object>());
  }

  public Map<?,?> asMap() {
    return asAd().intoMap(new HashMap<String,Object>());
  }

  // Helper function to try to find an unmarshal method.
  private <C> Method getUnmarshalMethod(Class<C> c) {
    try {
      try {
        // Look for a unmarshalling method which takes the stored type.
        return c.getMethod("unmarshal", object.getClass());
      } catch (NoSuchMethodException e) {
        // Look for a unmarshalling method that takes an object.
        return c.getMethod("unmarshal", Object.class);
      }
    } catch (NoSuchMethodException e) {
      return null;
    }
  }

  // Helper method for converting primitive classes to their wrappers.
  private static Class<?> fixPrimitiveClass(Class<?> c) {
    if (c == int.class)
      return Integer.class;
    if (c == double.class)
      return Double.class;
    if (c == float.class)
      return Float.class;
    if (c == byte.class)
      return Byte.class;
    if (c == long.class)
      return Long.class;
    if (c == short.class)
      return Short.class;
    if (c == boolean.class)
      return Boolean.class;
    if (c == char.class)
      return Character.class;
    return c;
  }

  public Object as(Class<?> c) {
    if (object == null) {
      return null;
    } if (c.isPrimitive()) {
      c = fixPrimitiveClass(c);
    } try {
      // Check if it's an array.
      if (c.isArray())
        return c.cast(asArray(c.getComponentType()));
      // Try a primitive type conversion.
      Method m = conv_map.get(c);
      if (m != null)
        return c.cast(m.invoke(this));
      // Try looking for an unmarshalling method.
      m = getUnmarshalMethod(c);
      if (m != null)
        return c.cast(m.invoke(null, object));
      // Just unmarshal the ad into the object.
      return asAd().unmarshalAs(c);
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(c.toString(), e);
    }
  }

  public <C> C[] asArray(Class<C> c) {
    if (object instanceof Ad) {
      Method m = conv_map.get(c);
      if (m == null)
        throw new RuntimeException("cannot convert to "+c);

      Ad ad = asAd();
      @SuppressWarnings("unchecked")
      C[] arr = (C[]) Array.newInstance(c, ad.size());

      try {
        int i = 0;
        if (ad.isList()) for (AdObject o : ad.list())
          Array.set(arr, i++, m.invoke(o));
        else if (ad.isMap()) for (AdObject o : ad.map().values())
          Array.set(arr, i++, m.invoke(o));
        return arr;
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      
    } else {
      @SuppressWarnings("unchecked")
      C[] arr = (C[]) Array.newInstance(c, 1);
      Array.set(arr, 0, as(c));
      return arr;
    }
  }

  // Check if the enclosed object is of a given type.
  public boolean is(Class<?> c) {
    return c.isInstance(object);
  }

  public boolean equals(Object o) {
    if (o == null)
      return false;
    if (o == this)
      return true;
    if (o instanceof AdObject)
      return object.equals(((AdObject)o).object);
    return false;
  }

  public int hashCode() {
    return object.hashCode();
  }

  // Compare two objects lexicographically.
  public int compareTo(AdObject o) {
    if (this.equals(o))
      return 0;
    if (object instanceof String)
      return asString().compareTo(o.asString());
    if (object instanceof Number)
      return (int) Math.signum(
        asNumber().doubleValue()-o.asNumber().doubleValue());
    if (object instanceof Boolean)
      return asBoolean().compareTo(o.asBoolean());
    if (object instanceof Ad)
      return 0; //return asAd().compareTo(o.asAd());
    return hashCode() - o.hashCode();
  }
}
