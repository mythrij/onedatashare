package stork.util;

// A problem which cannot be recovered from.

@SuppressWarnings("serial")
public class FatalEx extends RuntimeException {
  public FatalEx(Throwable cause) {
    super(cause.getMessage(), cause);
  } public FatalEx(String message, Throwable cause) {
    super(message, cause);
  } public FatalEx(String message) {
    super(message);
  }
}
