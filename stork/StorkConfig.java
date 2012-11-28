package stork;

import stork.*;
import stork.util.*;

import java.util.*;
import java.io.*;

// A class to read a config file, parse command line options, and allow
// the configuration information to be retrieved.

public class StorkConfig extends ClassAd {
  public final File file;
  private static StorkConfig instance = null;
  public static final GetOpts opts = new GetOpts();

  // Initialize the default command line parser.
  static {
    opts.add('c', "conf", "specify custom path to stork.conf").parser =
      opts.new SimpleParser("conf", "PATH", false);
    opts.add('p', "port", "set the port for the Stork server").parser =
      opts.new SimpleParser("port", "PORT", false);
    opts.add('h', "host", "set the host for the Stork server").parser =
      opts.new SimpleParser("host", "HOST", false);
    opts.add("help", "display this usage information");
    opts.add('q', "quiet", "don't print anything to standard output");

    opts.foot = new String[] {
      "Stork is still undergoing testing and development, "+
      "so please excuse the mess! If you encounter any bugs, "+
      "please contact Brandon Ross <bwross@buffalo.edu>."
    };
  }

  private static String[] default_paths = {
    System.getenv("STORK_CONFIG"),
    System.getProperty("stork.config"),
    System.getProperty("stork.exedir", ".")+"/stork.conf",
    "./stork.conf",
    "../stork.conf",
    "/usr/local/stork/stork.conf"
  };

  // Check default paths until we find a readable file. Null if none found.
  public static File defaultPath() {
    File f;
    for (String s : default_paths) if (s != null) {
      f = checkPath(s);
      if (f != null) return f;
    } return null;
  }

  // Check that path is readable and return absolutized file, else null.
  private static File checkPath(String path) {
    File file = new File(path).getAbsoluteFile();
    return file.canRead() ? file : null;
  }

  // Parse command line arguments, setting config values accordingly.
  // Then parse config file, where each line is either a comment or a
  // ClassAd expression which gets merged into the config ad.
  public StorkConfig(String path) throws Exception {
    // Initialize some defaults.
    insert("port", 38924);
    insert("libexec", "../libexec");

    if (path != null)
      file = checkPath(path);
    else
      file = defaultPath();

    // Error checking
    if (file == null) {
      if (path != null)
        throw new Exception("Error: couldn't open '"+path+"'");
      System.out.println("Warning: STORK_CONFIG not set and couldn't "+
                         "find stork.conf in default locations");
    } else if (!file.canRead()) {
      System.out.println("Warning: couldn't open config file '"+file+"'");
    } else {
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
