package stork;

import stork.*;
import stork.scheduler.*;
import stork.util.*;

import java.util.*;
import java.io.*;

// Handles parsing config files and command line arguments (or rather
// handles delegating command line parsing, to be precise) and initing
// and running Stork commands according to passed arguments.

public class StorkMain {
  public static final Ad def_env = new Ad();
  public static final GetOpts opts = new GetOpts();

  public static final int DEFAULT_PORT = 57024;

  // Try to get the version and build time from the build tag.
  public static String version() {
    String app = "JStork", ver = "", bts = "unknown";
    try {
      Properties props = new Properties();
      props.load(StorkMain.class.getResourceAsStream("/build_tag"));
      app = props.getProperty("appname", app);
      ver = props.getProperty("version", ver);
      bts = props.getProperty("buildtime", bts);
    } catch (Exception e) {
      /* Who cares... */
    } finally {
      return StorkUtil.join(app, ver, '('+bts+')');
    }
  }

  static {
    // Initialize the default environment.
    def_env.put("port", DEFAULT_PORT);
    def_env.put("libexec", "../libexec/");

    // Initialize the default command line parser.
    opts.prog = "StorkMain.class";
    opts.args = new String[] { "<command> [args...]" };
    opts.desc = new String[] {
      "Protip: you shouldn't be running me directly! :)" };

    opts.add('C', "conf", "specify custom path to stork.conf").parser =
      opts.new SimpleParser("conf", "PATH", false);
    opts.add('p', "port", "set the port for the Stork server").parser =
      opts.new SimpleParser("port", "PORT", false);
    opts.add('h', "host", "set the host for the Stork server").parser =
      opts.new Parser() {  // How can we handle IPv6?
        public String arg() { return "[host[:port]]"; }
        public Ad parse(String s) throws Exception {
          int i = s.lastIndexOf(':');
          String h = (i < 0) ? s    : s.substring(0,i);
          String p = (i < 0) ? null : s.substring(i+1);
          return new Ad().put("host", h).put("port", p);
        }
      };
    opts.add('V', "version", "display the version number and exit");
    opts.add("help", "display this usage information");
    opts.add('q', "quiet", "don't print anything to standard output");

    opts.foot = new String[] {
      "Stork is still undergoing testing and development, "+
      "so please excuse the mess! If you encounter any bugs, "+
      "please contact Brandon Ross <bwross@buffalo.edu>.", version()
    };
  }

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
  private static File defaultConfig() {
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

  // Parse config file, where each line is either a comment or a
  // Ad expression which gets merged into the returned ad.
  private static Ad parseConfig(String path) throws Exception {
    File file = (path != null) ? checkPath(path) : defaultConfig();

    // Error checking
    if (file == null) {
      if (path != null)
        throw new Exception("couldn't open '"+path+"'");
      throw new Exception("Warning: STORK_CONFIG not set and couldn't "+
                          "find stork.conf in default locations");
    } else if (!file.canRead()) {
      System.out.println("Warning: couldn't open config file '"+file+"'");
      return new Ad();
    }

    Reader r = new FileReader(file);
    Scanner sc = new Scanner(r).useDelimiter("\\A");

    if (!sc.hasNext())
      throw new Exception("couldn't read from config file: '"+file+"'");
    return Ad.parse("[\n"+sc.next()+"\n]");
  }

  // Main entry point. The first argument should be the command to
  // run (either stork_server or one of the client commands) and the
  // rest of the arguments will be put into a new array and passed
  // to relevant parsers.
  public static void main(String[] args) {
    String cmd = null;
    Ad env = def_env;
    GetOpts opt2;

    // Parse the first argument (command name).
    try {
      cmd = args[0];
      args = Arrays.copyOfRange(args, 1, args.length);
    } catch (Exception e) {
      opts.usageAndExit(1, "no command given");
    }

    // Little hacky...
    if (cmd.equals("stork_server")) {
      opt2 = StorkScheduler.getParser(opts);
    } else {
      opt2 = StorkClient.getParser(cmd, opts);
    } if (opt2 == null) {
      opts.usageAndExit(1, "invalid command: "+cmd);
    }

    // Parse command line options.
    try {
      env = opt2.parse(args).merge(env);
    } catch (Exception e) {
      opt2.usageAndExit(1, e.getMessage());
    }

    // Check if --help was specified.
    if (env.has("help")) {
      opt2.usageAndExit(0, null);
    } else if (env.has("version")) {
      System.out.println(version());
      System.exit(1);
    }

    // Try to parse config file and arguments.
    try {
      env = env.merge(parseConfig(env.get("conf")));
      env = env.merge(opts.parse(args));
    } catch (Exception e) {
      System.out.println("Error: "+e.getMessage());
    }

    // XXX: Stupid way to remove options from args to pass to Stork
    // client handler. Remove me when GetOpts handles positional args.
    int i = 0;
    for (String s : args) {
      if (s.startsWith("-")) i++;
      else break;
      if (s.equals("--")) break;
    } args = Arrays.copyOfRange(args, i, args.length);

    // Now run the actual command. (Again, a little hacky.)
    if (cmd.equals("stork_server")) {
      String host = env.get("host", "127.0.0.1");
      int port = env.getInt("port", DEFAULT_PORT);

      StorkScheduler s = StorkScheduler.instance(env);
      NettyStuff.TcpInterface si;

      try {
        si = new NettyStuff.TcpInterface(s, host, port);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
        return;
      }

      System.exit(s.waitFor());
    } else {
      StorkClient c = new StorkClient(env);
      c.execute(cmd, args);
    }
  }
}
