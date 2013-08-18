package stork.util;

import java.util.logging.*;
import static java.util.logging.Level.*;

// A slightly more convenient logging utility.

public abstract class Log {
  private final static Logger log = Logger.getAnonymousLogger();

  // Convenience logging methods that take variadic arguments.
  public static void log(Level l, Object... o) {
    if (log.isLoggable(l)) {
      // Get the caller.
      int i;
      StackTraceElement[] st = Thread.currentThread().getStackTrace();
      for (i = 1; i < st.length; i++) {
        if (!st[i].getClassName().equals(Log.class.getName()))
          break;
      }

      LogRecord lr = new LogRecord(l, StorkUtil.joinWith("", o));

      if (i < st.length) {
        lr.setSourceClassName(st[i].getClassName());
        lr.setSourceMethodName(st[i].getMethodName());
      }

      log.log(lr);
    }
  } public static void finest(Object... o) {
    log(FINEST, o);
  } public static void finer(Object... o) {
    log(FINER, o);
  } public static void fine(Object... o) {
    log(FINE, o);
  } public static void config(Object... o) {
    log(CONFIG, o);
  } public static void info(Object... o) {
    log(INFO, o);
  } public static void warning(Object... o) {
    log(WARNING, o);
  } public static void severe(Object... o) {
    log(SEVERE, o);
  }
}
