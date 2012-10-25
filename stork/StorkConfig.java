package stork;

import stork.*;
import stork.util.*;

import java.util.*;
import java.io.*;
import condor.classad.AttrName;
import condor.classad.ClassAdParser;
import condor.classad.ClassAdWriter;
import condor.classad.Constant;
import condor.classad.Expr;
import condor.classad.RecordExpr;

// A class to read a config file, parse command line options, and allow
// the configuration information to be retrieved.

public class StorkConfig extends ClassAd {
  private static String[] default_paths = {
    "./stork.conf",
    "../stork.conf",
    "/usr/local/stork/stork.conf"
  };

  // Called from constructor to set some default values used in Stork.
  private void init_defaults() {
    insert("port", 38924);
    insert("libexec", "/home/bwross/test/jstork/libexec");  // FIXME
  }

  // Determine the location of the config file. Returns null if nothing can
  // be found.
  public static File default_path() {
    File file;

    // Check environment...
    String path = System.getenv("STORK_CONFIG");

    if (path != null)
      return new File(path).getAbsoluteFile();

    // Check default locations...
    for (String s : default_paths) {
      file = new File(s).getAbsoluteFile();
      if (file.canRead()) return file;
    }

    return null;
  }

  // Parse a config file, where each line is either a comment or a
  // ClassAd expression which gets merged into the config ad.
  public void parseConfig(File file) throws Exception {
    LineNumberReader lnr = new LineNumberReader(new FileReader(file));
    String line;

    // Error checking
    if (file == null)
      throw new Exception("STORK_CONFIG not set and couldn't "+
                          "find stork.conf in default locations");
    if (!file.canRead())
      throw new Exception("Couldn't open config file '"+file.getName()+"'");

    // Read whole file.
    while ((line = lnr.readLine()) != null) {
      // Trim whitespace.
      line = line.trim();

      // Ignore comments and empty lines.
      if (line.length() == 0 || line.charAt(0) == '#') continue;

      // Interpret line as a ClassAd expression.
      ClassAd ad = parse("["+line+"]");
      
      // Error checking.
      if (ad == null)
        throw new Exception("Error parsing config file at line "+
                            lnr.getLineNumber());

      importAd(ad);
    }
  }

  // Parse with string path.
  public void parseConfig(String path) throws Exception {
    init_defaults();
    parseConfig(new File(path));
  }

  // Parse file at default path.
  public void parseConfig() throws Exception {
    parseConfig(default_path());
  }

  // Constructor: initialize defaults
  public StorkConfig() {
    init_defaults();
  }
}
