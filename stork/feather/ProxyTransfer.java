package stork.feather;

import java.util.*;

import stork.feather.util.*;

/**
 * A mediator for a locally proxied data transfer.
 *
 * @param <S> the source {@code Resource} type.
 * @param <D> the destination {@code Resource} type.
 */
public class ProxyTransfer<S extends Resource<?,S>, D extends Resource<?,D>>
extends Transfer<S,D> {
  // Queue for pending transfers.
  private Queue<Pending> queue = new LinkedList<Pending>();

  private static class Pending {
    final Bell bell;
    final Path path;
    protected Pending(Path path) {
      this(new Bell(), path);
    } protected Pending(Bell bell, Path path) {
      this.bell = bell;
      this.path = path;
    }
  }

  private volatile int ongoing = 0;

  /**
   * Create a {@code ProxyTransfer} that will transfer from {@code source} to
   * {@code destination}.
   *
   * @param source the source {@code Resource}.
   * @param destination the destination {@code Resource}.
   */
  public ProxyTransfer(S source, D destination) {
    super(source, destination);
  }

  protected Bell start() {
    System.out.println("Starting transfer...");
    return transfer(Path.ROOT);
  }

  protected void stop() {
    System.out.println("Transfer complete.");
  }

  private synchronized boolean canStartTransfer() {
    int c = concurrency();
    return c <= 0 || ongoing < c;
  }

  // Transfer a resource.
  private synchronized Bell transfer(final Path path) {
    final S src  = source.select(path);
    final D dest = destination.select(path);
    if (!canStartTransfer())
      return enqueueTransfer(path);
    ongoing++;
    return src.stat().new Promise() {
      public void then(Stat stat) {
        Bell bell;
        if (stat.link != null)
          bell = Bell.rungBell();
        else if (stat.dir)
          bell = dest.mkdir().and(transfer(path, src.list()));
        else if (stat.file)
          bell = transfer(src.tap(), dest.sink());
        else
          bell = Bell.rungBell();
        bell.as(stat).promise(this);
      } public void done() {
      } public void fail(Throwable t) {
        System.out.println("Failed to transfer: "+path);
        t.printStackTrace();
        synchronized (ProxyTransfer.this) { ongoing--; }
      }
    };
  }

  private synchronized Bell enqueueTransfer(Path path) {
    Pending pending = new Pending(path);
    queue.add(pending);
    return pending.bell;
  }

  private synchronized void popTransfer() {
    if (canStartTransfer()) {
      Pending pending = queue.poll();
      if (pending != null)
        transfer(pending.path).promise(pending.bell);
      else
        stopper.ring();
    }
  }

  // Transfer from tap to sink.
  private Bell transfer(Tap<S> tap, Sink<D> sink) {
    Pipe pipe = new Pipe() {
      protected void finish() {
        synchronized (ProxyTransfer.this) { ongoing--; }
        popTransfer();
        super.finish();
      }
    };
    return tap.attach(pipe).attach(sink).tap().start().new Promise() {
      public void fail() {
        synchronized (ProxyTransfer.this) { ongoing--; }
      }
    };
  }

  // Transfer directory listing.
  private synchronized Bell transfer(final Path path, Emitter<String> list) {
    ongoing--;
    return list.new ForEach() {
      public void each(String name) {
        transfer(path.appendLiteral(name));
      } public void done() {
        popTransfer();
      } public void fail(Throwable t) {
        handleFail(t);
      }
    }.isFailed() ? list : Bell.rungBell();
  }

  // TODO
  private void handleFail(Throwable t) {
    popTransfer();
  }
}
