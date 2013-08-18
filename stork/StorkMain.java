package stork;

import stork.*;
import stork.scheduler.*;
import stork.util.*;
import stork.ad.*;

import java.util.*;
import java.io.*;

// Handles parsing config files and command line arguments (or rather
// handles delegating command line parsing, to be precise) and initing
// and running Stork commands according to passed arguments.

public class StorkMain {
  public static final Ad def_env;
  public static final GetOpts bare_opts;
  public static final GetOpts base_opts;

  public static final int DEFAULT_PORT = 57024;

  // Try to get the version and build time from the build tag.
  public static String version() {
    String app = "Stork", ver = "", bts = "unknown";
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
    def_env = new Ad();
    def_env.put("port", DEFAULT_PORT);
    def_env.put("libexec", "libexec");

    // Construct the bare command line parser.
    bare_opts = new GetOpts();
    bare_opts.prog = "stork";
    bare_opts.args = new String[] {
      "<command> [args]", "[option...]" };

    bare_opts.add('V', "version", "display the version number and exit");
    bare_opts.add("help", "display this usage information");

    bare_opts.foot = new String[] {
      "Stork is still undergoing testing and development. "+
      "If you encounter any bugs, please contact "+
      "Brandon Ross <bwross@buffalo.edu>.", version()
    };

    // Ugly part where we add command descriptions manually.
    bare_opts.addCommand("server", "start the Stork server");
    bare_opts.addCommand("q", "query the Stork queue");
    bare_opts.addCommand("status", "an alias for q");
    bare_opts.addCommand("rm", "cancel or unschedule a job");
    bare_opts.addCommand("info", "get state information from a Stork server");
    bare_opts.addCommand("raw", "send a raw command ad to a Stork server");
    bare_opts.addCommand("help", "display usage information for a command");

    // Construct the base command line parser for commands to extend.
    base_opts = new GetOpts().parent(bare_opts);
    base_opts.add('C', "conf", "specify custom path to stork.conf").parser =
      base_opts.new SimpleParser("conf", "PATH", false);
    base_opts.add('p', "port", "set the port for the Stork server").parser =
      base_opts.new SimpleParser("port", "PORT", false);
    base_opts.add('h', "host", "set the host for the Stork server").parser =
      base_opts.new Parser() {  // How can we handle IPv6?
        public String arg() { return "[host[:port]]"; }
        public Ad parse(String s) {
          int i = s.lastIndexOf(':');
          String h = (i < 0) ? s    : s.substring(0,i);
          String p = (i < 0) ? null : s.substring(i+1);
          return new Ad().put("host", h).put("port", p);
        }
      };
    base_opts.add('q', "quiet", "don't print anything to standard output");
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

  // Parse config file, where each line is either a comment or an
  // Ad expression which gets merged into the returned ad.
  private static Ad parseConfig(String path) {
    File file = (path != null) ? checkPath(path) : defaultConfig();

    // Error checking
    if (file == null) {
      if (path != null)
        throw new RuntimeException("couldn't open '"+path+"'");
      throw new RuntimeException("STORK_CONFIG not set and "+
              "couldn't find stork.conf in default locations");
    } else if (!file.canRead()) {
      Log.warning("couldn't open config file '"+file+"'");
      return new Ad();
    }

    return Ad.parse(file, true);
  }

  // Main entry point. The first argument should be the command to
  // run (either stork_server or one of the client commands) and the
  // rest of the arguments will be put into a new array and passed
  // to relevant parsers.
  public static void main(String[] args) {
    String cmd = null;
    Ad env = def_env;
    GetOpts opt2;

    // Parse any options before the command name.
    // TODO: Find a better way to do this.
    int i = 0;
    for (String s : args) {
      if (s.startsWith("-")) i++;
      else break;
      if (s.equals("--")) break;
    } try {
      env = env.merge(bare_opts.parse(args));
    } catch (Exception e) {
      bare_opts.usageAndExit(1, e.getMessage());
    }

    // Check if version was specified.
    if (env.has("version")) {
      System.out.println(version());
      System.exit(0);
    }

    args = Arrays.copyOfRange(args, i, args.length);

    // Parse the first argument (command name).
    try {
      cmd = args[0];
      args = Arrays.copyOfRange(args, 1, args.length);
    } catch (Exception e) {
      bare_opts.usageAndExit(0, null);
    }

    // Check again if the command name is help.
    // FIXME: This is hecka hacky.
    if (cmd.equals("help")) {
      if (args.length > 0 && !args[0].startsWith("-")) {
        cmd = args[0];
        env.put("help", true);
      } else {
        bare_opts.usageAndExit(0, null);
      }
    }

    // Little hacky...
    if (cmd.equals("server")) {
      opt2 = StorkScheduler.getParser(base_opts);
    } else {
      opt2 = StorkClient.getParser(cmd, base_opts);
    } if (opt2 == null) {
      bare_opts.usageAndExit(1, "invalid command: "+cmd);
    }

    // Parse command line options.
    try {
      env = opt2.parse(args).merge(env);
    } catch (Exception e) {
      opt2.usageAndExit(1, e.getMessage());
    }

    // Check if help was specified.
    if (env.has("help")) {
      opt2.usageAndExit(0, null);
    }

    // Try to parse config file and arguments.
    try {
      env = env.merge(parseConfig(env.get("conf")));
      env = env.merge(opt2.parse(args));
    } catch (Exception e) {
      opt2.usageAndExit(1, e.getMessage());
    }

    // XXX: Stupid way to remove options from args to pass to Stork
    // client handler. Remove me when GetOpts handles positional args.
    i = 0;
    for (String s : args) {
      if (s.startsWith("-")) i++;
      else break;
      if (s.equals("--")) break;
    } args = Arrays.copyOfRange(args, i, args.length);

    if (env.getBoolean("quiet"))
      System.out.close();

    // Now run the actual command. (Again, a little hacky.)
    if (cmd.equals("server")) {
      String host = env.get("host");
      int port = env.getInt("port", DEFAULT_PORT);

      StorkScheduler s = StorkScheduler.start(env);
      NettyStuff.TcpInterface si;

      try {
        si = new NettyStuff.TcpInterface(s, host, port);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
        return;
      }

      //System.exit(s.waitFor());
    } else {
      StorkClient c = new StorkClient(env);
      c.execute(cmd, args);
    }
  }
}
