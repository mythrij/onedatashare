package stork.scheduler;

import stork.ad.*;
import stork.util.*;
import java.util.*;

// Represents a client request to be handled. Maintains a list
// of ads to be sent to the requestor.
// TODO: Refactor this.

public class RequestContext {
  public final Ad ad;
  public final String cmd;
  public StorkUser user = null;
  private Ad reply = null;
  private Bell<Ad> bell;

  public Ad status_ad = null;

  public RequestContext(Ad ad) {
    this(ad, null);
  } public RequestContext(Ad ad, Bell<Ad> bell) {
    cmd = ad.get("command");
    this.ad = ad.remove("command");
    this.bell = bell;
  }

  // Get the reply, if there is one. Optionally can block until there
  // is one.
  public synchronized Ad getReply() {
    return getReply(true);
  } public synchronized Ad getReply(boolean blocking) {
    if (reply == null && !blocking) {
      return null;
    } while (reply == null) try {
      wait();
    } catch (Exception e) {  // Interrupted...
      break;
    } return reply;
  }

  // Cancel the request.
  public synchronized void cancel() {
    cancel(null);
  } public synchronized void cancel(String reason) {
    if (reason == null)
      reason = "(unspecified)";
    reply(new Ad("error", reason));
  }

  // Check if the request has been served or canceled.
  public synchronized boolean isDone() {
    return reply != null;
  }

  // Called when the request is done being served.
  public synchronized void reply(Ad msg) {
    if (reply != null) {
      throw new RuntimeException("reply was already called");
    } else if (msg == null) {
      cancel();
    } else {
      reply = msg;
      if (bell != null)
        bell.ring(reply);
      notifyAll();
    }
  }
}
