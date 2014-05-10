package stork.feather.util;

import stork.feather.*;

/**
 * A {@code Crawler} crawls and operates on {@code Resource} trees. The
 * crawling proceeds in an order according to some traversal mechanism and
 * optionally produces a result. {@code Crawler} extends {@code Bell}, and will
 * ring with the root {@code Resource} when crawling has finished.
 *
 * @param <R> The {@code Resource} type this {@code Crawler} operates on.
 */
public abstract class Crawler<R extends Resource> extends Bell<R> {
  private final R root;
  private final Path pattern;
  private int mode = 0;
  private boolean started = false;

  /**
   * Create a {@code Crawler} which will perform operations on the physical
   * resources represented by {@code resource}. {@code resource} may be a
   * non-singleton {@code Resource} in which case all resources it represents
   * will be visited and operated on. Collection resources will not be operated
   * on recursively.
   *
   * @param resource the {@code Resource} to traverse.
   */
  public Crawler(R resource) { this(resource, false); }

  /**
   * Create a {@code Crawler} which will perform operations on the physical
   * resources represented by {@code resource}. {@code resource} may be a
   * non-singleton {@code Resource} in which case all resources it represents
   * will be visited and operated on. Collection resources will be operated on
   * recursively if {@code recursive} is {@code true}.
   *
   * @param resource the {@code Resource} to traverse.
   */
  public Crawler(R resource, boolean recursive) {
    root = resource.trunk();
    pattern = recursive ? resource.path().append("**") : resource.path();
  }

  /**
   * Start the crawling process.
   */
  public synchronized void start() {
    if (!started)
      crawl(root);
    started = true;
  }

  private void crawl(final R resource) {
    if (!isDone()) resource.subresources().new Promise() {
      public void done(Resource[] subs) {
        // Don't continue if crawling has completed.
        if (Crawler.this.isDone())
          return;

        // Perform the operation on the resource.
        operate(resource);

        // Crawl subresources that match the path.
        if (subs != null) for (Resource s : subs)
          if (pattern.intersects(s.path())) crawl(s);
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
   */
  protected abstract void operate(R resource);
}
