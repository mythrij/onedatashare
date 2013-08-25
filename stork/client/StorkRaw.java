package stork.client;

import stork.*;
import stork.ad.*;

import java.io.*;

// Handler for sending a raw command ad to the server.

public class StorkRaw extends StorkClient {
  public StorkRaw() {
    super("raw");

    args = new String[] { "[ad_file]" };
    desc = new String[] {
      "Send a raw command ad to a server, for debugging purposes.",
      "If no input ad is specified, reads from standard input."
    };
  }

  public Ad fillCommand(Ad ad) {
    if (args.length > 0) try {
      return Ad.parse(new FileInputStream(args[0]));
    } catch (Exception e) {
      throw new RuntimeException("couldn't read ad from file");
    } else try {
      System.out.print("Type a command ad:\n\n");
      return Ad.parse(System.in);
    } catch (Exception e) {
      throw new RuntimeException("couldn't read ad from stream");
    }
  }
      
  public void handle(Ad ad) {
    System.out.println(ad);
  }
}
