package stork.module;

// Unchecked exceptions which contain a message and a boolean value
// indicating whether the error is transient. It is meant to be
// lightweight and does not fill a stack trace.

@SuppressWarnings("serial")
public class ModuleException extends RuntimeException {
  private static StackTraceElement[] EMPTY = new StackTraceElement[0];
  private final boolean fatal;

  public ModuleException(boolean fatal) {
    this(fatal, null, null);
  } public ModuleException(boolean fatal, Throwable cause) {
    this(fatal, null, cause);
  } public ModuleException(boolean fatal, String message) {
    this(fatal, message, null);
  } public ModuleException(boolean fatal, String message, Throwable cause) {
    super(message, cause);
    this.fatal = fatal;
  }

  public Throwable fillInStackTrace() { return this; }

  public StackTraceElement[] getStackTrace() {
    return EMPTY;
  }

  public boolean isFatal() {
    return fatal;
  } public boolean isTransient() {
    return !fatal;
  }

  // This will include wrapped exceptions as well.
  public String getMessage() {
    String m = super.getMessage();
    Throwable t = super.getCause();
    return (m == null && t == null) ? "(unknown)" :
           (m == null && t != null) ? t.getMessage() :
           (m != null && t != null) ? m+": "+t.getMessage() : m;
  }

  // It's recommended that these be imported by client classes and used to
  // throw these exceptions, instead of constructing and throwing them.
  public static ModuleException abort() {
    return new ModuleException(true);
  } public static ModuleException abort(Throwable cause) {
    return new ModuleException(true, cause);
  } public static ModuleException abort(String message) {
    return new ModuleException(true, message);
  } public static ModuleException abort(String message, Throwable cause) {
    return new ModuleException(true, message, cause);
  } public static ModuleException abort(boolean fatal) {
    return new ModuleException(fatal);
  } public static ModuleException abort(boolean fatal, Throwable cause) {
    return new ModuleException(fatal, cause);
  } public static ModuleException abort(boolean fatal, String message) {
    return new ModuleException(fatal, message);
  } public static ModuleException abort(boolean f, String m, Throwable c) {
    return new ModuleException(f, m, c);
  }
}
