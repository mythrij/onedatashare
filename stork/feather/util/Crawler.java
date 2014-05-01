package stork.feather.util;

import stork.feather.*;

/**
 * A {@code Crawler} operates on a {@code Resource} tree recursively. The
 * crawling proceeds in an order according to some traversal mechanism and
 * optionally produces a result. {@code Crawler} extends {@code Bell}, and will
 * ring with the result when crawling has finished.
 */
public abstract class Crawler<T> extends Bell<T> {
  private final Resource root;
  private final Path pattern;
  private int mode = 0;
  private boolean started = false;

  /**
   * Create a {@code Crawler} which will perform operations recursively on the
   * {@code Resource} tree specified by the given {@code Resource}. If {@code
   * resource} is a singleton {@code Resource}, the operation will be performed
   * only on that {@code Resource}.
   *
   * @param resource the {@code Resource} to traverse.
   */
  public Crawler(Resource resource) {
    root = resource.trunk();
    pattern = resource.path();
  }

  /**
   * Start the crawling process.
   */
  public synchronized void start() {
    if (!started)
      crawl(root);
    started = true;
  }

  private void crawl(final Resource resource) {
    if (!isDone()) resource.subresources().new Promise() {
      public void done(Resource[] subs) {
        // Don't continue if crawling has completed.
        if (Crawler.this.isDone())
          return;

        // Perform the operation on the resource.
        operate(resource, stat);

        // Crawl subresources that match the path.
        if (subs != null) for (Resource s : subs)
          if (patterns.intersects(s.path())) crawl(s);
      } public void fail(Throwable t) {
        Crawler.this.ring(t);
      }
    };
  }

  /**
   * Subclasses should override this to implement an operation that will be
   * performed on {@code resource}.
   *
   * @param resource the {@code Resource} to operate one.
   * @param stat the {@code Stat} of {@code resource}.
   */
  protected abstract void operate(Resource resource);
}
