package stork.ad;

import java.lang.reflect.*;

public interface Unmarshaller<T> {
  // Unmarshal the ad into an instance of a class. May be passed a base
  // instance of the class, if one is present (e.g. when unmarshalling into
  // a field which is already set) or null. This method will also be passed
  // generic parameters, if present, on a best-effort basis.
  public T unmarshal(Ad ad, T base, Type... generics);
}
