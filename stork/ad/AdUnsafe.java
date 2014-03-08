package stork.ad;

import java.lang.reflect.*;

/**
 * A (semi-)cross-platform utility class for creating objects without calling
 * their constructor. This class is based on a similar class used in Google's
 * GSON library. It is anticipated that Java 8 will have a standardized way to
 * access {@code Unsafe}, in which case we might deprecate this class.
 */
final class AdUnsafe {

  // The unsafe allocation method.
  private static AdMember allocator;
  private static int type = findType(), ocid = 0;

  // Determine the unsafe allocator.
  //   0 = No Unsafe on system.
  //   1 = Sun's Unsafe.
  //   2 = Pre-Gingerbread Dalvik.
  //   3 = Post-Gingerbread Dalvik.
  private static int findType() {
    // Try to find Sun's Unsafe class.
    try {
      allocator = new AdType(Class.forName("sun.misc.Unsafe"))
        .method("allocateInstance", Class.class);
      if (allocator != null) return 1;
    } catch (Exception e) {}

    // Try to find pre-Gingerbread ObjectInputStream's newInstance.
    try {
      allocator = new AdType(java.io.ObjectInputStream.class)
        .method("newInstance", Class.class, Class.class);
      if (allocator != null) return 2;
    } catch (Exception e) {}

    // Try to find post-Gingerbread ObjectInputStream's newInstance.
    try {
      AdType ois = new AdType(java.io.ObjectInputStream.class);
      ocid = (Integer) ois.method("getConstructorId", Class.class)
        .invoke(null, Object.class);
      allocator = ois.method("newInstance", Class.class, int.class);
      if (allocator != null) return 3;
    } catch (Exception e) {}

    // Guess we can't do unsafe creation...
    return 0;
  }

  /**
   * Try to create a new instance of a class without invoking its constructor.
   *
   * @param c the class of the object to create
   * @return An unconstructed instance of {@code T}, or {@code null} if
   * unconstructed instantiation could not be performed.
   */
  public static synchronized <T> T create(Class<T> c) {
    try {
      switch (type) {
        case 1: return c.cast(allocator.invoke(null, c));
        case 2: return c.cast(allocator.invoke(null, c, Object.class));
        case 3: return c.cast(allocator.invoke(null, c, ocid));
      }
    } catch (Exception e) { }
    return null;
  }
}
