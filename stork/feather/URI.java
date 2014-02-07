package stork.feather;

public class URI {
  public java.net.URI uri;

  public URI(String uri) {
    this.uri = java.net.URI.create(uri);
  }

  public static URI create(String uri) {
    return new URI(uri);
  }

  // Return an escaped version of the given string.
  public static String escape(String string) {
    return string;
  }

  // Return an unescaped version of the given escaped string.
  public static String unescape(String string) {
    return string;
  }
}
