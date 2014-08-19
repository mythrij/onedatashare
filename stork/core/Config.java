package stork.core;

import java.io.*;

import stork.ad.*;
import stork.feather.*;
import stork.util.*;

/** A class for storing configuration settings. */
public class Config {
  /** Global configuration. */
  public static final Config global = new Config();

  public int max_jobs = 10;
  public int max_attempts = 10;
  public int max_history = 10;

  //public String libexec = "libexec";

  public String state_file = null;
  public int state_save_interval = 120;

  public URI connect = URI.create("tcp://localhost:57024");
  public URI[] listen;
  public URI web_service_url;

  public boolean registration = false;

  public double request_timeout = 5.0;

  // Default locations for stork.conf.
  private static String[] default_paths = {
    System.getenv("STORK_CONFIG"),
    System.getProperty("stork.config"),
    System.getProperty("stork.exedir", ".")+"/stork.conf",
    "./stork.conf",
    "../stork.conf",
    "/usr/local/stork/stork.conf"
  };

  // Check default paths until we find a readable file. Null if none found.
  private File defaultConfig() {
    File f;
    for (String s : default_paths) if (s != null) {
      f = checkPath(s);
      if (f != null) return f;
    } return null;
  }

  // Check that path is readable and return absolutized file, else null.
  private File checkPath(String path) {
    File file = new File(path).getAbsoluteFile();
    return file.canRead() ? file : null;
  }

  // Parse config file, where each line is either a comment or an
  // Ad expression which gets merged into the returned ad.
  private Ad parseConfig(String path) {
    File file = (path != null) ? checkPath(path) : defaultConfig();

    // Error checking
    if (file == null) {
      if (path != null)
        throw new RuntimeException("Couldn't open '"+path+"'");
      throw new RuntimeException("STORK_CONFIG not set and "+
              "couldn't find stork.conf in default locations");
    } else if (!file.canRead()) {
      Log.warning("Couldn't open config file '"+file+"'");
      return new Ad();
    }

    return Ad.parse(file, true);
  }

  /** Find the config file, open and parse it, and unmarshal settings. */
  public void loadConfig(String path) {
    parseConfig(path).unmarshal(this);
  }
}
