package stork;

import stork.*;
import stork.util.*;

import java.util.*;
import java.io.*;

// A class to read a config file, parse command line options, and allow
// the configuration information to be retrieved.

public class StorkConfig extends ClassAd {
  public final File file;

  private static String[] default_paths = {
    System.getenv("STORK_CONFIG"),
    System.getProperty("stork.config"),
    System.getProperty("stork.exedir", ".")+"/stork.conf",
    "./stork.conf",
    "../stork.conf",
    "/usr/local/stork/stork.conf"
  };

  // Determine the location of the config file. Returns null if nothing can
  // be found.
  public static File defaultPath() {
    // Check default locations...
    for (String s : default_paths) if (s != null) {
      File file = new File(s).getAbsoluteFile();
      if (file.canRead()) return file;
    } return null;
  }

  // Parse command line arguments, setting config values accordingly.
  // Then parse config file, where each line is either a comment or a
  // ClassAd expression which gets merged into the config ad.
  public StorkConfig(String[] args) throws Exception {
    // Initialize some defaults.
    insert("port", 38924);
    insert("libexec", "../libexec");

    // TODO: Parse config file path from command line.
    File file = null;

    if (file == null)
      file = defaultPath();

    this.file = file;

    // Error checking
    if (file == null) {
      System.out.println("Warning: STORK_CONFIG not set and couldn't "+
                         "find stork.conf in default locations");
    } else if (!file.canRead()) {
      System.out.println("Warning: couldn't open config file '"+file+"'");
    } else {
      System.out.println("Config file: "+file);

      LineNumberReader lnr = new LineNumberReader(new FileReader(file));
      String line;

      // Read whole file.
      while ((line = lnr.readLine()) != null) {
        // Trim whitespace.
        line = line.trim();

        // Ignore comments and empty lines.
        if (line.length() == 0 || line.charAt(0) == '#') continue;

        // Interpret line as a ClassAd expression.
        ClassAd ad = parse("["+line+"]");

        // Error checking.
        if (ad.error())
          throw new Exception("couldn't parse config file at line "+
              lnr.getLineNumber());

        importAd(ad);
      }
    }
  }

  public StorkConfig() throws Exception {
    this(null);
  }
}
