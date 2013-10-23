package stork.ad;

// Utility marshallers and unmarshallers.

public final class Marshallers {
  // I'm a utility class!
  private Marshallers() { }

  // Create a marshaller that calls a zero-parameter method on the
  // passed class to generate its marshalled form. Throws a runtime
  // exception during marshalling if the method does not exist.
  public static Marshaller methodMarshaller(final String mn) {
    return new Marshaller() {
      public Object marshal(Object object) {
        try {
          return object.getClass().getMethod(mn).invoke(object);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  // Just marshal anything passed to this using its toString method.
  public static final Marshaller STRING_MARSHALLER =
    methodMarshaller("toString");

  // Marshal anything as a null literal.
  public static final Marshaller NULL_MARSHALLER = new Marshaller() {
    public Object marshal(Object o) {
      return null;
    }
  };
}
