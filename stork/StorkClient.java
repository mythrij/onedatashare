package stork;

import stork.*;
import stork.util.*;

import java.io.*;
import java.net.*;
import java.util.*;

public class StorkClient {
  private Socket server_sock;

  private static Map<String, StorkCommand> cmd_handlers;

  // Configuration variables
  private ClassAd env = null;

  // Some static initializations...
  static {
    // Initialize command handlers
    cmd_handlers = new HashMap<String, StorkCommand>();
    cmd_handlers.put("stork_q", new StorkQHandler());
    cmd_handlers.put("stork_list", new StorkQHandler());
    cmd_handlers.put("stork_submit", new StorkSubmitHandler());
    cmd_handlers.put("stork_rm", new StorkRmHandler());
    cmd_handlers.put("stork_info", new StorkInfoHandler());
  }

  // Client command handlers
  // -----------------------
  // The handle method should return a ClassAd containing the response
  // from the server.
  private static abstract class StorkCommand {
    InputStream is;
    OutputStream os;

    public ResponseAd handle(ClassAd env, String[] args, Socket sock) {
      try {
        is = sock.getInputStream();
        os = sock.getOutputStream();

        // Some sanity checking
        if (is == null || os == null)
          throw new Exception("problem with socket");

        return _handle(env, args);
      } catch (Exception e) {
        return new ResponseAd("error", e.getMessage());
      }
    }

    abstract GetOpts parser(GetOpts base);
    abstract ResponseAd _handle(ClassAd env, String[] args) throws Exception;
  }

  private static class StorkQHandler extends StorkCommand {
    GetOpts parser(GetOpts base) {
      GetOpts opts = new GetOpts(base);

      opts.prog = "stork_q";
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
      opts.add('f', "follow",
               "retrieve list every N seconds (default: 2)").parser =
        opts.new SimpleParser("limit", "[N]", true);

      return opts;
    }

    ResponseAd sendRequest(ClassAd ad) throws Exception {
      int received = 0, expecting;

      os.write(ad.getBytes());
      os.flush();

      // Read and print ClassAds until response ad is found
      for (ad = ClassAd.parse(is); !ad.has("response"); received++) {
        if (ad.error())
          return new ResponseAd(ad);

        System.out.print(ad+"\n\n");
        ad = ClassAd.parse(is);
      }

      // We're done once we get a response ad.
      String msg = null;
      ResponseAd res = new ResponseAd(ad);

      expecting = res.getInt("count");
      String not_found = res.get("not_found");

      // Report how many ads we received.
      if (expecting >= 0 && received != expecting) {
        msg = "Warning: expecting "+expecting+" job ad(s), "+
              "but received "+received+"!\n";
      } else if (received > 0) {
        msg = "Received "+received+" job ad(s)";
        if (not_found != null)
          msg += ", but some jobs not found: "+not_found+"\n";
        else
          msg += ".\n";
      } else if (res.success()) {
        msg = "No jobs found...\n";
      }

      if (msg != null) System.out.println(msg);

      return res;
    }

    ResponseAd _handle(ClassAd env, String[] args) throws Exception {
      ClassAd ad = new ClassAd();
      Range range = new Range();
      String status = null;
      int follow = env.getInt("follow");

      ad.insert("command", "stork_q");

      // Parse arguments
      for (String s : args) {
        Range r = Range.parseRange(s);
        if (r == null) {
          if (s == args[0])
            status = s;
          else
            return new ResponseAd("error", "invalid argument: "+s);
        } else {
          range.swallow(r);
        }
      }

      if (!range.isEmpty())
        ad.insert("range", range.toString());
      if (status != null)
        ad.insert("status", status);

      // If we're following, keep resending every interval.
      // TODO: Make sure clearing is portable.
      if (follow > 0) while (true) {
        System.out.print("\033[H\033[2J");
        ResponseAd r = sendRequest(ad);
        if (!r.success()) return r;
        Thread.sleep(follow*1000);
      } else {
        return sendRequest(ad);
      }
    }
  }

  private static class StorkRmHandler extends StorkCommand {
    GetOpts parser(GetOpts base) {
      GetOpts opts = new GetOpts(base);

      opts.prog = "stork_rm";
      opts.args = new String[] { "[option...] [job_id...]" };
      opts.desc = new String[] {
        "This command can be used to cancel pending or running jobs on "+
        "a Stork server.", "The job id, of which there may be more than "+
        "one, may be either an integer or a range of the form: "+
        "m[-n][,range] (e.g. 1-4,7,10-13)"
      };

      return opts;
    }

    ResponseAd _handle(ClassAd env, String[] args) throws Exception {
      Range range = new Range();
      ClassAd ad = new ClassAd();
      ad.insert("command", "stork_rm");

      // Arg check
      if (args.length < 1)
        return new ResponseAd("error", "not enough arguments");

      // Parse arguments
      for (String s : args) {
        Range r = Range.parseRange(s);
        if (r == null)
          return new ResponseAd("error", "invalid argument: "+s);
        range.swallow(r);
      }

      ad.insert("range", range.toString());

      // Write request ad
      os.write(ad.getBytes());
      os.flush();

      // Read and print ClassAds until response ad is found
      ad = ClassAd.parse(is);

      if (ad == null || !ResponseAd.is(ad))
        return new ResponseAd("error", "invalid response ad received");
      else
        return new ResponseAd(ad);
    }
  }

  private static class StorkInfoHandler extends StorkCommand {
    GetOpts parser(GetOpts base) {
      GetOpts opts = new GetOpts(base);

      opts.prog = "stork_info";
      opts.args = new String[] { "[option...] [type]" };
      opts.desc = new String[] {
        "This command retrieves information about the server itself, "+
        "such as transfer modules available and server statistics.",
        "Valid options for type: modules (default), server"
      };

      return opts;
    }

    ResponseAd _handle(ClassAd env, String[] args) throws Exception {
      ClassAd ad = new ClassAd();
      ad.insert("command", "stork_info");

      // Set the type of info to get from the server
      if (args.length > 0)
        ad.insert("type", args[0]);
      else
        ad.insert("type", "module");

      // Write request ad
      os.write(ad.getBytes());
      os.flush();

      // Read and print ClassAds until response ad is found
      while (true) {
        ad = ClassAd.parse(is);

        if (ad.error())
          return new ResponseAd("error", "couldn't parse ad from server");

        if (!ad.has("response"))
          System.out.println(ad);
        else
          return new ResponseAd(ad);
      }
    }
  }

  private static class StorkSubmitHandler extends StorkCommand {
    GetOpts parser(GetOpts base) {
      GetOpts opts = new GetOpts(base);

      opts.prog = "stork_submit";
      opts.args = new String[] {
        "[option...]",
        "[option...] [job_file]",
        "[option...] [src_url] [dest_url]"
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

        "After each job is submitted, stork_submit outputs the job "+
        "id, assuming it was submitted successfully.",

        "(Note about x509 proxies: stork_submit will check if "+
        "\"x509_file\" is included in the submit ad, and, if so, "+
        "read the proxy file, and include its contents in the job ad "+
        "as \"x509_proxy\". This may be removed in the future.)"
      };

      return opts;
    }

    // Print the submission response ad in a nice way.
    private void print_response(ResponseAd ad) {
      // Make sure we have a response ad.
      if (ad == null)
        ad = new ResponseAd("error", "couldn't parse server response");
      else if (!ad.has("response"))
        ad = new ResponseAd("error", "invalid response from server");

      // Check if the job was successfully submitted.
      else if (ad.success()) {
        System.out.print("Job accepted and assigned id: ");
        System.out.println(ad.get("job_id"));
        System.out.println(ad);
      }

      // It wasn't successfully submitted! If we know why, explain.
      else {
        System.out.print("Job submission failed! Reason: ");
        System.out.println(ad.get("message"));
      }
    }

    // Attempt to submit a job and return a response ad.
    private ResponseAd submit_job(ClassAd ad) {
      ad.insert("command", "stork_submit");

      // Replace x509_proxy in job ad.
      // TODO: A better way of doing this would be nice...
      String proxy = ad.get("x509_file");
      ad.remove("x509_file");

      if (proxy != null) try {
        File f = new File(proxy);
        Scanner s = new Scanner(f);
        StringBuffer sb = new StringBuffer();

        while (s.hasNextLine())
          sb.append(s.nextLine()+"\n");
        
        if (sb.length() > 0)
          ad.insert("x509_proxy", sb.toString());
      } catch (Exception e) {
        System.out.println("Couldn't open x509_file...");
      }

      // Write ad
      try {
        os.write(ad.getBytes());
        os.flush();
      } catch (Exception e) {
        return new ResponseAd("error", e.getMessage());
      }

      return new ResponseAd(ClassAd.parse(is));
    }

    // Submit multiple ads from a stream, return ad reporting statistics.
    private ResponseAd submit_from_stream(InputStream in, boolean print) {
      ClassAd ad;
      ResponseAd res;
      int js = 0, ja = 0;  // jobs sent and jobs accepted

      while (true) {
        ad = ClassAd.parse(in);

        // Check if we've reached the end.
        if (ad == ClassAd.EOF) break;

        // Check if ad was properly formatted.
        if (ad.error()) {
          System.out.println("\nError: malformed input ad; nothing submitted\n");
          continue;
        }

        if (print) System.out.println(ad);

        // Submit job and check response.
        print_response(res = submit_job(ad));

        js++;

        if (res.success()) ja++;
      }

      // Report number of submissions that were accepted. If none were
      // accepted, return an error.
      return new ResponseAd(ja > 0 ? "success" : "error",
                            ja+" of "+js + " jobs successfully submitted");
    }

    ResponseAd _handle(ClassAd env, String[] args) throws Exception {
      ClassAd ad;
      ResponseAd res;
      Console cons;

      switch (args.length) {
        case 0:  // From stdin
          cons = System.console();

          // Check if we're running on a console first. If so, give
          // them the fancy prompt. TODO: use readline()
          if (cons != null) {
            System.out.println("Begin typing ClassAd (ctrl-D to end):");
            return submit_from_stream(System.in, false);
          } else {
            return submit_from_stream(System.in, true);
          }
        case 1:  // From file
          FileInputStream fis = new FileInputStream(new File(args[0]));
          return submit_from_stream(fis, true);
        case 2:  // src_url and dest_url
          ad = new ClassAd();
          ad.insert("src_url", args[0]);
          ad.insert("dest_url", args[1]);
          print_response(res = submit_job(ad));
          return res;
        default:
          return new ResponseAd("error", "wrong number of arguments");
      }
    }
  }

  // Class methods
  // -------------
  // Get a GetOpts parser for a Stork command.
  public static GetOpts getParser(String cmd, GetOpts base) {
    try {
      return cmd_handlers.get(cmd).parser(base);
    } catch (Exception e) {
      return null;
    }
  }

  // Connect to a Stork server.
  public void connect(String host, int port) throws Exception {
    server_sock = new Socket(host, port);
  }

  // Execute a command on the connected Stork server.
  public void execute(String cmd, String[] args) {
    StorkCommand scmd = cmd_handlers.get(cmd);
    String host = env.get("host");
    int port = env.getInt("port");

    // Make sure we have a command handler by that name.
    if (scmd == null) {
      System.out.println("unknown command: "+cmd);
      return;
    }

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
    ResponseAd ad = scmd.handle(env, args, server_sock);
    System.out.println("Done: "+ad.toDisplayString());
  }

  public StorkClient(ClassAd env) {
    this.env = env;
  }
}
