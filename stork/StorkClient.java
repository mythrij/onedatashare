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
  private static StorkConfig conf = new StorkConfig();

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

    public ResponseAd handle(String[] args, Socket sock) {
      try {
        is = sock.getInputStream();
        os = sock.getOutputStream();

        // Some sanity checking
        if (is == null || os == null)
          throw new Exception("problem with socket");

        if (args.length < 1)
          throw new Exception("too few args passed to handler");

        return _handle(args);
      } catch (Exception e) {
        return new ResponseAd("error", e.getMessage());
      }
    }

    abstract ResponseAd _handle(String[] args) throws IOException;
  }

  private static class StorkQHandler extends StorkCommand {
    ResponseAd _handle(String[] args) throws IOException {
      int received = 0, expecting;
      ClassAd ad = new ClassAd();
      Range range = new Range();
      String not_found;
      String status = null;

      ad.insert("command", "stork_q");

      // Parse arguments
      for (String s : args) {
        if (s == args[0]) continue;
        Range r = Range.parseRange(s);
        if (r == null)
          status = s;
        else
          range.swallow(r);
      }

      if (!range.isEmpty())
        ad.insert("range", range.toString());
      if (status != null)
        ad.insert("status", status);

      // Write request ad
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
      not_found = res.get("not_found");

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
  }

  private static class StorkRmHandler extends StorkCommand {
    ResponseAd _handle(String[] args) throws IOException {
      ClassAd ad = new ClassAd();
      ad.insert("command", "stork_rm");

      // Arg check
      if (args.length != 2)
        return new ResponseAd("error", "not enough arguments");

      /// TODO Range parsing here too
      ad.insert("range", args[1]);

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
    ResponseAd _handle(String[] args) throws IOException {
      ClassAd ad = new ClassAd();
      ad.insert("command", "stork_info");

      // Set the type of info to get from the server
      if (args.length > 1) {
        ad.insert("type", args[1]);
      }

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

    ResponseAd _handle(String[] args) throws IOException {
      ClassAd ad;
      ResponseAd res;
      Console cons;

      switch (args.length) {
        case 1:  // From stdin
          cons = System.console();

          // Check if we're running on a console first. If so, give
          // them the fancy prompt. TODO: use readline()
          if (cons != null) {
            System.out.println("Begin typing ClassAd (ctrl-D to end):");
            return submit_from_stream(System.in, false);
          } else {
            return submit_from_stream(System.in, true);
          }
        case 2:  // From file
          FileInputStream fis = new FileInputStream(new File(args[1]));
          return submit_from_stream(fis, true);
        case 3:  // src_url and dest_url
          ad = new ClassAd();
          ad.insert("src_url", args[1]);
          ad.insert("dest_url", args[2]);
          print_response(res = submit_job(ad));
          return res;
        default:
          return new ResponseAd("error", "wrong number of arguments");
      }
    }
  }

  // Class methods
  // -------------
  private ResponseAd send_command(String cmd, String[] args) throws Exception {
    ResponseAd ad, res = null;

    try {
      ad = cmd_handlers.get(cmd).handle(args, server_sock);
    } catch (Exception e) {
      throw new Exception("Unknown command: "+cmd);
    }

    // Print response ad
    System.out.println("Done. "+ad.toDisplayString());

    return ad;
  }

  // Constructor
  // -----------
  // TODO Remove socket logic from constructor.
  public StorkClient(InetAddress host, int port) throws IOException {
    if (host != null)
      server_sock = new Socket(host, port);
    else
      server_sock = new Socket("127.0.0.1", port);
  }

  public StorkClient(int p) throws IOException {
    this(null, p);
  }

  public static void main(String[] args) {
    StorkClient client;
    String cmd;

    // Parse config file
    try {
      conf = new StorkConfig();

      // Parse arguments
      if (args.length < 1)
        throw new Exception("Must give client command");

      // Get command. TODO: Recheck arguments
      cmd = args[0];

      // Connect to Stork server.
      try {
        client = new StorkClient(conf.getInt("port"));
      } catch (Exception e) {
        throw new Exception("Couldn't connect to server: "+e);
      }

      // Send command to server.
      try {
        ClassAd ad = client.send_command(cmd, args);
        System.exit(ad.get("response").equals("success") ? 0 : 1);
      } catch (Exception e) {
        throw new Exception("Couldn't send "+cmd+": "+e);
      }
    } catch (Exception e) {
      System.out.println("Error: "+e.getMessage());
      System.exit(1);
    }
  }
}
