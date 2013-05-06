package stork.ad;

// Ad sink to allow for ads from multiple sources.

public class AdSink {
  volatile boolean closed = false;
  volatile boolean more = true;
  volatile Ad ad = null;

  public synchronized void close() {
    closed = true;
    System.out.println("Closing ad sink...");
    notifyAll();
  }

  public synchronized void putAd(Ad ad) {
    if (closed) return;
    this.ad = ad;
    notifyAll();
  }

  public synchronized void mergeAd(Ad a) {
    putAd((ad != null) ? ad.merge(a) : a);
  }

  // Block until an ad has come in, then clear the ad.
  public synchronized Ad getAd() {
    if (!closed && more) try {
      wait();
      if (ad == null) return null;
      if (closed) more = false;
      return new Ad(ad);
    } catch (Exception e) { }
    return null;
  }

  // Get the ad current in the sink, or an empty ad if none.
  public synchronized Ad peekAd() {
    return (ad != null) ? new Ad(ad) : new Ad();
  }
}
