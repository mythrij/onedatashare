package stork.ad;

import java.util.*;
import java.math.*;
import java.lang.reflect.*;

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
    map(Double.class,  "asObject");
    map(Float.class,   "asFloat");
    map(Byte.class,    "asByte");
    map(Long.class,    "asLong");
    map(Short.class,   "asShort");
    map(Boolean.class, "asBoolean");
    map(int.class,     "asInt");
    map(double.class,  "asObject");
    map(float.class,   "asFloat");
    map(byte.class,    "asByte");
    map(long.class,    "asLong");
    map(short.class,   "asShort");
    map(boolean.class, "asBooleanValue");
    map(java.net.URI.class, "asURI");
  }

  private AdObject(Object o) {
    object = o;
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
    return object.toString();
  } public String toString() {
    return object.toString();
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
    throw new RuntimeException("cannot convert to ad");
  }

  public <C extends Object> C as(Class<C> c) {
    Method m = conv_map.get(c);
    if (m == null) {
      throw new RuntimeException("cannot convert to "+c+" from "+object.getClass());
    } try {
      return c.cast(m.invoke(this));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <C> C[] asList(Class<C> c) {
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
  public boolean is(Class c) {
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
