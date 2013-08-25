package stork;

import stork.scheduler.*;
import stork.ad.*;
import stork.util.*;
import java.net.URI;

// This is basically a standalone wrapper around StorkScheduler that lets
// the scheduler run as a standalone process (as opposed to as a client
// of another JVM). This will create server interfaces for the scheduler.

public class StorkServer extends Command {
  public StorkServer() {
    super("server");

    args = new String[] { "[option]..." };
    desc = new String[] {
      "The Stork server is the core of the Stork system, handling "+
        "connections from clients and scheduling transfers. This command "+
        "is used to start a Stork server.",
        "Upon startup, the Stork server loads stork.conf and begins "+
          "listening for clients."
    };
    add('d', "daemonize", "run the server in the background, "+
      "redirecting output to a log file (if specified)");
    add('l', "log", "redirect output to a log file at PATH").new
      SimpleParser("PATH", true);
    add("state", "load/save server state at PATH").new
      SimpleParser("PATH", true);
  }

  public void execute(Ad env) {
    // TODO: Ugh, this is really idiotic, but we can deal with it later.
    env.unmarshal(Stork.settings);
    env.addAll(Ad.marshal(Stork.settings));

    StorkScheduler s = StorkScheduler.start(env);
    URI[] listen = Stork.settings.listen;

    if (listen == null || listen.length < 1)
      listen = new URI[] { Stork.settings.connect };

    for (URI u : listen) try {
      NettyStuff.createInterface(s, u);
    } catch (Exception e) {
      e.printStackTrace();
      Log.warning("could not create interface: "+e.getMessage());
    }
  }
}
