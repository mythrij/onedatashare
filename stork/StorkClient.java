package stork;

import stork.*;
import stork.ad.*;
import stork.util.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.text.*;

// All the client commands tossed into one file. TODO: Refactor.

public class StorkClient {
  private Socket server_sock;

  private static Map<String, Class<? extends StorkCommand>> cmd_handlers;

  // Configuration variables
  private Ad env = null;

  // Some static initializations...
  static {
    // Initialize command handlers
    cmd_handlers = new HashMap<String, Class<? extends StorkCommand>>();
    cmd_handlers.put("q", StorkQHandler.class);
    cmd_handlers.put("status", StorkQHandler.class);
    cmd_handlers.put("submit", StorkSubmitHandler.class);
    cmd_handlers.put("rm", StorkRmHandler.class);
    cmd_handlers.put("info", StorkInfoHandler.class);
    cmd_handlers.put("ls", StorkLsHandler.class);
    cmd_handlers.put("raw", StorkRawHandler.class);
  }

  // Client command handlers
  // -----------------------
  static abstract class StorkCommand {
    Socket sock = null;
    InputStream is = null;
    OutputStream os = null;
    Ad env = null;
    String[] args = null;

    final void init(Ad env, String[] args) {
      this.env = env;
      this.args = args;
    }

    final Ad handle(Socket sock) {
      try {
        this.sock = sock;
        is = sock.getInputStream();
        os = sock.getOutputStream();
        Ad ad;

        // Some sanity checking
        if (env == null || args == null)
          throw new Error("handler was not initialized");
        if (is == null || os == null)
          throw new Exception("problem with socket");

        // Write command ad to the server.
        do {
          ad = command();

          // Write command to server.
          os.write((ad.toString()+"\n").getBytes("UTF-8"));
          os.flush();

          handle(ad = Ad.parse(is));
          return ad;
        } while (hasMoreCommands());
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        complete();
      }
    }

    // Override this if the handler sends multiple command ads.
    public boolean hasMoreCommands() {
      return false;
    }

    // Return a parser for parsing command line options.
    abstract public GetOpts parser();

    // Return the command ad to send to the server.
    abstract public Ad command();

    // Handle each response from the server.
    abstract public void handle(Ad ad);

    // Anything else that needs to be done at the end of a command.
    public void complete() { }
  }

  // Handler for performing remote listings.
  static class StorkLsHandler extends StorkCommand {
    String uri = null;
    boolean all = false;
    boolean long_fmt = false;

    public GetOpts parser() {
      GetOpts opts = new GetOpts();
      opts.prog = "ls";
      opts.args = new String[] { "[option...] <url>" };
      opts.desc = new String[] {
        "This command can be used to list the contents of a remote "+
        "directory."
      };
      opts.add('d', "depth", "list only up to N levels").parser =
        opts.new SimpleParser("depth", "N", false);
      opts.add('r', "recursive", "recursively list subdirectories");
      opts.add('a', "all", "include hidden directories in listing");
      opts.add('l', "long", "show long form listing information");

      return opts;
    }

    public Ad command() {
      Ad ad = new Ad("command", "ls");

      // Check args.
      if (args.length < 1)
        throw new RuntimeException("not enough arguments");
      else if (args.length > 1)
        throw new RuntimeException("too many argument arguments");
      ad.put("uri", uri = args[0]);

      // Check for options.
      if (env.getBoolean("recursive"))
        ad.put("depth", env.getInt("depth", -1));
      all = env.getBoolean("all");
      long_fmt = env.getBoolean("long");
      return ad;
    }

    // Flatten the tree structure for listing.
    private static List<Ad> flatten(List<Ad> list, Ad ad) {
      list.add(ad);
      if (ad.has("files")) for (Ad a : ad.getAds("files"))
        if (a.has("files")) flatten(list, a);
      return list;
    }

    // Print a single directory listing ad.
    private void printListing(Ad ad) {
      Ad[] files = ad.getAds("files");
      if (files == null || files.length == 0)
        return;
      if (files.length > 1)
        System.out.println(ad.get("name")+":");
      for (Ad a : files)
        printEntry(a);
    }

    // Helper methods for printing long listing information.
    static DateFormat df1 = new SimpleDateFormat("MMM dd HH:mm");
    static DateFormat df2 = new SimpleDateFormat("MMM dd  yyyy");
    static String dateString(long time) {
      if (time < 0) return "(unknown)";
      // Why is this so atrocious in Java.
      Date d = new Date(time*1000);
      Calendar c = Calendar.getInstance();
      c.setTime(d);
      int dy = c.get(Calendar.YEAR);
      int cy = Calendar.getInstance().get(Calendar.YEAR);
      DateFormat fmt = (cy == dy) ? df1 : df2;
      return fmt.format(d);
    }

    // Print a single list entry. Pretty icky.
    private void printEntry(Ad ad) {
      String name = ad.get("name");
      if (name == null)
        return;
      if (!all && name.startsWith("."))
        return;
      if (ad.getBoolean("dir"))
        name += '/';

      if (long_fmt) {
        // Get the permissing string.
        String p = ad.get("perm", "?");
        // Format the date.
        String d = dateString(ad.getLong("time"));
        // Get the size.
        String s = StorkUtil.prettySize(ad.getLong("size", 0));
        System.out.printf("%9s %6s %12s %s\n", p, s, d, name);
      } else {
        System.out.println(name);
      }
    }

    public void handle(Ad ad) {
      if (ad.has("error"))
        return;

      // Print everything from the list.
      for (Ad a : flatten(new LinkedList<Ad>(), ad))
        printListing(a);
    }
  }

  static class StorkQHandler extends StorkCommand {
    boolean count_only = false;
    boolean raw = false;
    Ad cmd = null;

    public GetOpts parser() {
      GetOpts opts = new GetOpts();
      opts.prog = "q";
      opts.args = new String[] { "[option...] [status] [job_id...]" };
      opts.desc = new String[] {
        "This command can be used to query a Stork server for information "+
        "about jobs in queue.", "Specifying status allows filtering "+
        "of results based on job status, and may be any one of the " +
        "following values: pending (default), all, done, scheduled, "+
        "processing, removed, failed, or complete.", "The job id, of "+
        "which there may be more than one, may be either an integer or "+
        "a range of the form: m[-n][,range] (e.g. 1-4,7,10-13)"
      };
      opts.add('c', "count", "print only the number of results");
      opts.add('n', "limit", "retrieve at most N results").parser =
        opts.new SimpleParser("limit", "N", false);
      opts.add('r', "reverse", "reverse printing order (oldest first)");
      opts.add('w', "watch",
               "fetch queue every T seconds (default 2)").parser =
        opts.new SimpleParser("watch", "T", true);
      opts.add("daglog", "output results to FILE in DAGMan log format")
        .parser = opts.new SimpleParser("daglog", "FILE", false);

      return opts;
    }

    public Ad command() {
      Ad ad = new Ad();
      Ad res;
      Range range = new Range();
      String status = null;
      int watch = -1;

      if (env.has("watch"))
        watch = env.getInt("watch", 2);

      ad.put("command", "q");

      // Check command line options.
      if (env.getBoolean("count"))
        ad.put("count", count_only = true);
      if (env.getBoolean("reverse"))
        ad.put("reverse", true);

      // Parse arguments
      for (String s : args) {
        Range r = Range.parseRange(s);
        if (r == null) {
          if (s == args[0])
            status = s;
          else
            return new Ad("error", "invalid argument: "+s);
        } else {
          range.swallow(r);
        }
      }

      if (!range.isEmpty())
        ad.put("range", range.toString());
      if (status != null)
        ad.put("status", status);
      return cmd = ad;
    }

    // Print a job ad in a nice and pretty format.
    private void printTableHeader() {
      System.out.printf("%3s  %-12s  %8s  %8s  %9s  %s\n",
                        "ID", "Status", "Queue", "Run", "Size", "Speed");
      System.out.printf("%3s  %-12s  %8s  %8s\n", "", "", "time", "time");
    } private String time(Ad ad) {
      Watch w = ad.unmarshalAs(Watch.class);
      return w.toString();
    } private String progressBar(Ad ad, int len) {
      Progress prog = ad.unmarshalAs(Progress.class);
      int j = (prog.total > 0) ? (int)(len * prog.done / prog.total) : 0;
      char[] bar = new char[len];

      if (j >= len)
        j = len-1;

      for (int i = 0; i < len; i++)
        bar[i] = ' ';
      for (int i = 0; i <= j; i++)
        bar[i] = '=';
      if (j+1 < len)
        bar[j+1] = '>';
      return "    ["+new String(bar)+"] "+prog.toPercent();
    } private void formatJobAd(Ad ad) {
      System.out.printf("%3d  %-12s  %8s  %8s  %9s  %s\n",
        ad.getInt("job_id"), ad.get("status", "(unknown)"),
        time(ad.getAd("queue_timer")), time(ad.getAd("run_timer")),
        StorkUtil.prettySize(ad.getLong("progress.bytes.total")),
        Throughput.pretty(ad.getLong("progress.bytes.avg")));
      System.out.println(progressBar(ad.getAd("progress.bytes"), 50));

      System.out.println("    "+ad.get("src.uri"));
      System.out.println("    "+ad.get("dest.uri"));

      if (ad.has("message"))
        System.out.println("    Message: "+ad.get("message"));

      System.out.println();
    }

    public void handle(Ad ad) {
      // Check if we just wanted the count.
      if (count_only) {
        if (ad.has("error"))  // Should we print instead?
          System.out.println(0);
        else if (ad.has("count"))
          System.out.println(ad.getInt("count"));
        return;
      }

      // Print this so the user knows what exactly was requested.
      if (cmd.has("status")) {
        System.out.print("Searching for "+cmd.get("status")+" jobs");
        if (cmd.has("range"))
          System.out.println(" with ID(s): "+cmd.get("range")+"\n");
        else
          System.out.println("...\n");
      } else if (cmd.has("range")) {
        System.out.println("Searching for jobs with ID(s): "+cmd.get("range")+"\n");
      }

      // Check if there was an error.
      if (ad.has("error"))
        throw new FatalEx(ad.get("error"));
      if (!ad.has("jobs"))
        throw new FatalEx("unexpected reply format");

      // Print all the job ads. TODO: Better formatting.
      if (!raw) {
        printTableHeader();
        for (Ad a : ad.getAds("jobs"))
          formatJobAd(a);
      } else {
        System.out.println(ad);
      }

      // Report how many ads we received.
      int count = ad.getInt("count");
      String not_found = ad.get("not_found");

      if (count > 0) {
        String msg = "Received "+count+" job ad(s)";
        if (not_found != null)
          msg += ", but some jobs were not found: "+not_found;
        else
          msg += ".";
        System.out.println(msg);
      } else {
        System.out.println("No jobs found...");
      }

      // If we're watching, keep resending every interval.
      // TODO: Make sure clearing is portable.
      /*
      if (watch > 0) while (true) {
        System.out.print("\033[H\033[2J");
        System.out.print("Querying every "+watch+" sec...\n\n");
        res = sendRequest(ad);

        if (!res.success()) break;

        try {
          Thread.sleep(watch*1000);
        } catch (Exception e) { break; }
      } else {
        res = sendRequest(ad);
      } return res;
      */
    }
  }

  // Kind of stupidly hacked together thing that periodicially checks the
  // status of 

  static class StorkRmHandler extends StorkCommand {
    public GetOpts parser() {
      GetOpts opts = new GetOpts();
      opts.prog = "rm";
      opts.args = new String[] { "[option...] [job_id...]" };
      opts.desc = new String[] {
        "This command can be used to cancel pending or running jobs on "+
        "a Stork server.", "The job id, of which there may be more than "+
        "one, may be either an integer or a range of the form: "+
        "m[-n][,range] (e.g. 1-4,7,10-13)"
      };

      return opts;
    }

    public Ad command() {
      Range range = new Range();
      Ad ad = new Ad();
      ad.put("command", "rm");

      // Arg check.
      if (args.length < 1)
        throw new FatalEx("not enough arguments");

      // Parse arguments.
      for (String s : args) {
        Range r = Range.parseRange(s);
        if (r == null)
          throw new FatalEx("invalid argument: "+s);
        range.swallow(r);
      }

      ad.put("range", range.toString());
      return ad;
    }

    public void handle(Ad ad) {
      System.out.println(ad);
    }
  }

  static class StorkInfoHandler extends StorkCommand {
    public GetOpts parser() {
      GetOpts opts = new GetOpts();
      opts.prog = "info";
      opts.args = new String[] { "[option...] [type]" };
      opts.desc = new String[] {
        "This command retrieves information about the server itself, "+
        "such as transfer modules available and server statistics.",
        "Valid type arguments: modules (default), server"
      };

      return opts;
    }

    public Ad command() {
      Ad ad = new Ad();
      ad.put("command", "info");

      // Set the type of info to get from the server
      if (args.length > 0)
        ad.put("type", args[0]);
      else
        ad.put("type", "module");
      return ad;
    }

    public void handle(Ad ad) {
      System.out.println(ad);
    }
  }

  static class StorkSubmitHandler extends StorkCommand {
    private int submitted = 0, accepted = 0;

    public GetOpts parser() {
      GetOpts opts = new GetOpts();
      opts.prog = "submit";
      opts.args = new String[] {
        "[option...]",
        "[option...] <job_file>",
        "[option...] <src_url> <dest_url>"
      };
      opts.desc = new String[] {
        "This command is used to submit jobs to a Stork server. ",

        "If called with no arguments, prompts the user and reads job "+
        "ads from standard input.",

        "If called with one argument, assume it's a path to a file "+
        "containing one or more job ads, which it opens and reads.",

        "If called with two arguments, assumes they are a source "+
        "and destination URL, which it parses and generates a job "+
        "ad for.",

        "After each job is submitted, submit outputs the job "+
        "id, assuming it was submitted successfully.",

        "(Note about x509 proxies: submit will check if "+
        "\"x509_file\" is included in the submit ad, and, if so, "+
        "read the proxy file, and include its contents in the job ad "+
        "as \"x509_proxy\". This may be removed in the future.)"
      };
      opts.add('b', "brief", "print only submitted job IDs");

      return opts;
    }

    // Print the submission response ad in a nice way.
    private void print_response(Ad ad) {
      // If we're in Condor compatibility mode, print only the line Condor
      // expects and that's it.
      if (env.getBoolean("condor_mode"))
        System.out.println("Request assigned id: "+ad.get("job_id"));

      // Check if the job was successfully submitted.
      else if (!ad.has("error")) {
        System.out.print("Job accepted and assigned id: ");
        System.out.println(ad.get("job_id"));
        System.out.println(ad);
      }

      // It wasn't successfully submitted! If we know why, explain.
      else {
        System.out.print("Job submission failed! Reason: ");
        System.out.println(ad.get("message", "(unspecified)"));
      }
    }

    private boolean parsedArgs = false;
    private boolean echo = true;

    public Ad command() {
      Ad ad = null;

      // Determine if we're going to read from stream or generator our
      // own ad.
      if (!parsedArgs) switch (args.length) {
        default:
          throw new FatalEx("wrong number of arguments");
        case 2:  // src_url and dest_url
          ad = new Ad("src",  args[0]).put("dest", args[1]);
          break;
        case 1:  // From file
          ad = Ad.parse(new File(args[0]), true);
          break;
        case 0:  // From stdin
          if (System.console() != null) {
            echo = false;
            System.out.print("Begin typing submit ads (ctrl+D to end):\n\n");
          } ad = Ad.parse(System.in, true);
      } parsedArgs = true;

      // Check if this is an ad list with only one ad.
      if (ad.isList() && ad.size() == 1) {
        ad = ad.getAd(0);
      }

      ad.put("command", "submit");

      // Replace x509_proxy in job ad.
      // TODO: A better way of doing this would be nice...
      String proxy = ad.get("x509_file");
      ad.remove("x509_file");

      if (proxy != null) try {
        proxy = StorkUtil.readFile(proxy);

        if (proxy.length() > 0)
          ad.put("x509_proxy", proxy);
      } catch (Exception e) {
        System.out.println("Fatal: couldn't open x509_file...");
        System.exit(1);
      }

      // Print the command sent if we're echoing.
      if (echo)
        System.out.print(ad+"\n\n");
      return ad;
    }

    public void handle(Ad ad) {
      submitted++;
      if (!ad.has("error")) {
        accepted++;
      } if (env.getBoolean("quiet")) {
        // Do nothing.
      } else if (env.getBoolean("brief")) {
        for (Ad a : ad.getAds("jobs"))
          System.out.println(a.getInt("job_id"));
      } else {
        System.out.println(
          (accepted > 0 ? "Success: " : "Error: ") +
          accepted+" of "+submitted+" jobs successfully submitted");
      }
    }
  }

  // Handler for sending a raw command ad to the server.
  static class StorkRawHandler extends StorkCommand {
    public GetOpts parser() {
      GetOpts opts = new GetOpts();
      opts.prog = "raw";
      opts.args = new String[] { "[ad_file]" };
      opts.desc = new String[] {
        "Send a raw command ad to a server, for debugging purposes.",
        "If no input ad is specified, reads from standard input."
      };

      return opts;
    }

    public Ad command() {
      if (args.length > 0) try {
        return Ad.parse(new FileInputStream(args[0]));
      } catch (Exception e) {
        throw new FatalEx("couldn't read ad from file");
      } else try {
        System.out.print("Type a command ad:\n\n");
        return Ad.parse(System.in);
      } catch (Exception e) {
        throw new FatalEx("couldn't read ad from stream");
      }
    }
        
    public void handle(Ad ad) {
      System.out.println(ad);
    }
  }

  // Class methods
  // -------------
  // Get a command handler by command name or null if none.
  public static StorkCommand handler(String cmd) {
    try {
      return (StorkCommand) cmd_handlers.get(cmd).newInstance();
    } catch (Exception e) {
      return null;
    }
  }

  // Get a GetOpts parser for a Stork command.
  public static GetOpts getParser(String cmd, GetOpts base) {
    try {
      return handler(cmd).parser().parent(base);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  // Connect to a Stork server.
  public StorkClient connect(String host, int port) throws Exception {
    server_sock = new Socket(host, port);
    return this;
  }

  // Execute a command on the connected Stork server.
  public void execute(String cmd, String[] args) {
    StorkCommand scmd = handler(cmd);
    String host = env.get("host");
    int port = env.getInt("port", StorkMain.DEFAULT_PORT);

    // Make sure we have a command handler by that name.
    if (scmd == null) {
      System.out.println("unknown command: "+cmd);
      return;
    }

    // Initialize command handler. TODO: Merge args and env, also check
    // opts parser for errors.
    scmd.init(env, args);

    // Try to do connection stuff.
    if (server_sock == null) try {
      connect(host, port);
    } catch (Exception e) {
      if (host != null)
        System.out.println("Error: couldn't connect to "+host+":"+port);
      else
        System.out.println("Error: couldn't connect to localhost:"+port);
      System.exit(1);
    }

    // Execute the command handler.
    try {
      Ad ad = scmd.handle(server_sock);
      //Log.info("Got ad: ", ad);

      if (ad.has("error"))
        System.err.println("Error: "+ad.get("error"));
    } catch (Exception e) {
      //e.printStackTrace();
      System.err.println("Error: "+e.getMessage());
    }
  }


  public StorkClient() {
    this(new Ad());
  } public StorkClient(Ad env) {
    this.env = env;
  }
}
