package stork.scheduler;

import stork.ad.*;
import stork.util.*;
import java.util.*;

// Represents a client request to be handled. Maintains a list
// of ads to be sent to the requestor.

public class RequestContext {
  public final Ad ad;
  public final String cmd;
  private LinkedList<Ad> replies;

  private Bell<Ad> reply_bell, end_bell;

  private boolean is_done = false;
  public Ad status_ad = null;

  public RequestContext(Ad ad) {
    this(ad, null, null);
  } public RequestContext(Ad ad, Bell<Ad> reply_bell) {
    this(ad, reply_bell, null);
  } public RequestContext(Ad ad, Bell<Ad> reply_bell, Bell<Ad> end_bell) {
    cmd = ad.get("command");
    this.ad = ad.remove("command");
    this.reply_bell = reply_bell;
    this.end_bell = end_bell;
    replies = new LinkedList<Ad>();
  }

  // Put an ad in the reply queue.
  public synchronized void putReply(Ad ad) {
    if (!is_done) {
      ad = new Ad(ad).mode(this.ad.mode());
      replies.add(ad);
      if (reply_bell != null) reply_bell.ring(ad);
      notifyAll();
    }
  }

  // Get the topmost reply, optionally blocking until there is one. If
  // we're not blocking and there's no request, returns null. Also
  // returns null when the request is complete.
  public synchronized Ad getReply() {
    return getReply(true);
  } public synchronized Ad getReply(boolean blocking) {
    if (replies.isEmpty() && (is_done || !blocking)) {
      return null;
    } while (replies.isEmpty() && !is_done) try {
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
    if (!is_done) {
      status_ad = new Ad(last).mode(ad.mode());
      if (end_bell != null)
        end_bell.ring(status_ad);
      is_done = true;
    } notifyAll();
  }

  // Returns true if there will never be more ads to read.
  public synchronized boolean isDone() {
    return replies.size() <= 0 && is_done;
  }

  // Wait for the request to be complete. Returns the status ad.
  public synchronized Ad waitFor() {
    while (!is_done) try {
      wait();
    } catch (Exception e) {
      // Who cares...
    } return status_ad;
  }

  // Get the whole list of unread replies. Doesn't clear list.
  public synchronized List<Ad> getReplies() {
    return new LinkedList<Ad>(replies);
  }
}
