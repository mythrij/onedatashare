package stork.feather.net;

import java.nio.channels.*;
import java.util.*;

import stork.feather.*;

import static java.nio.channels.SelectionKey.*;

public class Selector<C extends SelectableChannel> {
  /** The channel associated with this selector. */
  private C channel;
  private SelectionKey key;

  private static boolean selecting;
  private static boolean inited;
  private static java.nio.channels.Selector nioSelector;

  private Bell<?> onAccept = new Bell<Void>();
  private Bell<?> onConnect = new Bell<Void>();
  private Bell<?> onRead = new Bell<Void>();
  private Bell<?> onWrite = new Bell<Void>();

  private static Thread thread = new Thread("Selector") {
    public void run() { while (true) select(); }
  };

  public Selector(C channel) {
    synchronized (Selector.class) {
      init();
      try {
        nioSelector.wakeup();
        key = channel.register(nioSelector, 0, this);
        this.channel = channel;
      } catch (Exception e) {
        throw new RuntimeException("Could not register", e);
      }
    }
  }

  private static synchronized void init() {
    if (!inited) try {
      nioSelector = java.nio.channels.Selector.open();
      thread.setDaemon(true);
      thread.start();
      inited = true;
    } catch (Exception e) {
      throw new RuntimeException("Could not initialize selector");
    }
  }

  /** Rings when there are incoming connections. */
  public final synchronized Bell<C> onAccept() {
    setInterest(OP_ACCEPT, true);
    return onAccept.as(channel);
  }

  /** Rings when the channel has finished connecting. */
  public final synchronized Bell<C> onConnect() {
    setInterest(OP_CONNECT, true);
    return onConnect.as(channel);
  }

  /** Rings when there is data waiting to be read. */
  public final synchronized Bell<C> onRead() {
    setInterest(OP_READ, true);
    return onRead.as(channel);
  }

  /** Rings when it's possible to write data. */
  public final synchronized Bell<C> onWrite() {
    setInterest(OP_WRITE, true);
    return onWrite.as(channel);
  }

  private synchronized void setInterest(int op, boolean interest) {
    if ((channel.validOps() & op) == 0)
      throw new IllegalArgumentException("Invalid operation");

    int from = key.interestOps();
    int to = interest ? (from | op) : (from & ~op);

    if (from != to) synchronized (Selector.class) {
      if (selecting) nioSelector.wakeup();
      key.interestOps(to);
    }
  }

  /** Do selection and fire event handler for each channel. */
  private static void select() {
    Set<SelectionKey> set = doNioSelect();
    for (SelectionKey k : doNioSelect())
      ((Selector) k.attachment()).handleEvents();
    set.clear();
  }

  private static Set<SelectionKey> doNioSelect() {
    try {
      synchronized (Selector.class) { selecting = true; }
      nioSelector.select();
      return nioSelector.selectedKeys();
    } catch (Exception e) {
      // If this happens, something has gone terribly wrong, i.e. someone
      // closed the NIO selector or there was an I/O error. In either case,
      // it means the whole system is broken.
      throw new Error();
    } finally {
      synchronized (Selector.class) { selecting = false; }
    }
  }

  int i = 0;
  /** Called when some event has occurred. */
  private synchronized void handleEvents() {
    if (key.isAcceptable()) {
      System.out.println("ACC");
      onAccept.ring();
      onAccept = new Bell<Void>();
    } if (key.isConnectable()) {
      System.out.println("CONN");
      onConnect.ring();
      onConnect = new Bell<Void>();
    } if (key.isReadable()) {
      System.out.println("READ "+i++);
      onRead.ring();
      onRead = new Bell<Void>();
    } if (key.isWritable()) {
      System.out.println("WRIT");
      onWrite.ring();
      onWrite = new Bell<Void>();
    }

    // We're no longer interested in events that just triggered.
    key.interestOps(key.interestOps() & ~key.readyOps());
  }
}
