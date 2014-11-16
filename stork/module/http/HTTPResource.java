package stork.module.http;

import stork.feather.Bell;
import stork.feather.Path;
import stork.feather.Resource;
import stork.feather.Slice;
import stork.feather.Stat;
import stork.feather.Tap;

/**
 * Stores the requested full {@link Path}, and state information of the 
 * connection. It creates {@link HTTPTap} instances.
 */
public class HTTPResource extends Resource<HTTPSession, HTTPResource> {

  // Rung when the first resource response header is received
  private Bell<Stat> statBell = new Bell<Stat> ();

  /**
   * Constructs a {@code resource} with HTTP connection request.
   * 
   * @param session the class where this request made from
   * @param path requested resource {@code path}
   */
  protected HTTPResource(HTTPSession session, Path path) {
    super(session, path);
  }

  public HTTPTap tap() {
    return new HTTPTap();
  }

  public synchronized Bell<Stat> stat() {
    return initialize().new AsBell<Stat>() {
      public Bell<Stat> convert(HTTPResource r) {
        if (statBell.isDone())
          return statBell;

        HTTPBuilder builder = session.builder;

        // We need to make a HEAD request.
        if (!builder.onCloseBell.isDone()) {
          HTTPChannel ch = builder.getChannel();

          ch.addChannelTask(new HTTPTap());  // FIXME hacky
          ch.writeAndFlush(builder.prepareHead(path));
        } else {
          statBell.ring(new HTTPException("Http session " +
                builder.getHost() + " has been closed."));
        }

        return statBell;
      }
    };
  }

  /**
   * This can be considered as a specific download task for the
   * request from a {@link HTTPResource}.
   */
  public class HTTPTap extends Tap<HTTPResource> {

    protected Bell<Void> onStartBell, sinkReadyBell;
    private HTTPBuilder builder;
    private Path resourcePath;

    /**
     * Constructs a {@code tap} associated with a {@code resource}
     * that receives data from HTTP connection.
     */
    public HTTPTap() {
      super(HTTPResource.this);
      this.builder = HTTPResource.this.session.builder;
      onStartBell = new Bell<Void> ();
      setPath(path);
    }

    public Bell<?> start(final Bell bell) {
      return initialize().and(bell).new AsBell() {
        public Bell convert(Object o) {
          if (builder.onCloseBell.isDone()) {
            return onStartBell.cancel();
          }
          sinkReadyBell = bell;

          synchronized (builder.getChannel()) {
            if (!builder.onCloseBell.isDone()) {
              HTTPChannel ch = builder.getChannel();

              if (builder.isKeepAlive()) {
                ch.addChannelTask(HTTPTap.this);
                ch.writeAndFlush(
                    builder.prepareGet(resourcePath));
              } else {
                builder.tryResetConnection(HTTPTap.this);
              }
            } else {
              onStartBell.ring(new HTTPException("Http session " +
                    builder.getHost() + " has been closed."));
            }
          }

          sinkReadyBell.new Promise() {
            public void fail(Throwable t) {
              onStartBell.ring(t);
              finish(t);
            }
          };

          return onStartBell;
        }
      };
    }

    public Bell<?> drain(Slice slice) {
      return super.drain(slice);
    }

    public void finish() { super.finish(); }

    /** 
     * Tells whether this {@code HTTPTap} instance has acquired
     * state info.
     */
    protected boolean hasStat() {
      return statBell.isDone();
    }

    /** Sets state info and rings its {@code state Bell}. */
    protected void setStat(Stat stat) {
      statBell.ring(stat);
    }

    /**
     * Reconfigures its {@code path}. 
     * 
     * @param path new {@link Path} instance to be changed to
     */
    protected void setPath(Path path) {
      resourcePath = path;
    }

    /*** Gets reconfigured {@code path}. */
    protected Path getPath() {
      return resourcePath;
    }
  }
}
