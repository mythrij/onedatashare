package stork.scheduler;

import stork.util.*;
import java.util.*;

// Represents a client request to be handled. Maintains a list
// of ads to be sent to the requestor.

public class RequestContext {
  private boolean isDone = false;
  public final Ad ad;
  public final String cmd;
  private LinkedList<Ad> replies;
  private Bell<Ad> bell;

  public RequestContext(Ad ad, Bell<Ad> b) {
    cmd = ad.get("command");
    this.ad = ad.remove("command");
    bell = b;
    replies = new LinkedList<Ad>();
  }

  // Put an ad in the reply queue.
  public synchronized void putReply(Ad ad) {
    if (!isDone) {
      replies.add(ad);
      if (bell != null) bell.ring(ad);
      notifyAll();
    }
  }

  // Get the topmost reply, optionally blocking until there is one. If
  // we're not blocking and there's no request, returns null. Also
  // returns null when the request is complete.
  public synchronized Ad getReply() {
    return getReply(true);
  } public synchronized Ad getReply(boolean blocking) {
    if (replies.isEmpty() && (isDone || !blocking)) {
      return null;
    } while (replies.isEmpty()) try {
      wait();
    } catch (Exception e) {
      // Whatever.
    } try {
      return replies.pop();
    } finally {
      notifyAll();
    }
  }

  // Called when the request is done being served.
  public synchronized void done(Ad last) {
    if (!isDone) {
      if (last != null)
        putReply(last);
      isDone = true;
      notifyAll();
    }
  }

  // Returns true if there will never be more ads to read.
  public synchronized boolean isDone() {
    return replies.size() <= 0 && isDone;
  }
}
