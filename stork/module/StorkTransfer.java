package stork.module;

import stork.ad.*;
import stork.util.*;

// Temporary stand-in until betterness becomes available.

public class StorkTransfer implements Runnable {
  SubmitAd ad;
  StorkSession sess;
  int rv = -1;
  AdSink sink = new AdSink();

  public StorkTransfer(StorkSession sess, SubmitAd ad) {
    this.sess = sess;
    this.ad = ad;
  }

  public void run() {
    try {
      sess.setSink(sink);
      sess.transfer(ad.src.getPath(), ad.dest.getPath());
      done(0);
    } catch (Exception e) {
      done(1);
    }
  }

  public void start() {
    new Thread(this).start();
  }

  public void stop() {
    // Ignore for now.
  }

  private synchronized void done(int rv) {
    this.rv = rv;
    notifyAll();
  }

  public synchronized int waitFor() {
    while (rv < 0) try {
      wait();
    } catch (Exception e) {
      return 1;
    } return rv;
  }

  public Ad getAd() {
    return sink.getAd();
  }
}
