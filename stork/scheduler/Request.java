package stork.scheduler;

import stork.ad.*;
import stork.user.*;
import stork.util.*;
import java.util.*;

// Represents a client request to be handled.

public class Request extends Bell.Single<Ad> {
  public final Ad ad;
  public final String cmd;
  public User user;
  public StorkScheduler.CommandHandler handler;
  private Ad reply = null;

  public Request(Ad ad) {
    cmd = ad.get("command");
    this.ad = ad.remove("command");
  }
}
