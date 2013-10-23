package stork.ad;

import java.lang.reflect.*;

public interface Marshaller<T> {
  // Marshal the given object into an ad-compatible format. The returned
  // object must be an ad primitive, or an exception will be thrown during
  // marshalling.
  public Object marshal(T object);
}
