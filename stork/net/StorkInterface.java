package stork.net;

import java.net.*;

import io.netty.bootstrap.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;

import stork.ad.*;
import stork.scheduler.*;
import stork.util.*;
import stork.feather.URI;

// An interface for clients to communicate with a Stork server.

public abstract class StorkInterface
extends ChannelInitializer<Channel> {
  protected String name;  // Used for debugging messages.
  protected final URI uri;
  protected Scheduler sched;
  private Channel chan;

  // Global connection selector.
  private static final EventLoopGroup acceptor = new NioEventLoopGroup();

  public StorkInterface(Scheduler sched, URI uri) {
    this.uri = uri;
    this.sched = sched;
  }

  // Automatically determine and create an interface from a URI.
  public static StorkInterface create(Scheduler s, URI u) {
    u = u.makeImmutable();
    String p = u.scheme();
    if (p == null)
      p = u.toString();
    if (p.equals("tcp"))
      return new TcpInterface(s, u);
    if (p.equals("http"))
      return HttpInterface.register(s, u);
    if (p.equals("https"))
      return HttpInterface.register(s, u);
    throw new RuntimeException("Unsupported interface scheme: "+p);
  }

  // Get the port from the URI, falling back to the default if none.
  private int getPortFromUri() {
    int port = uri.port();

    if (uri.scheme() == null)
      return defaultPort();
    if (port > 0)
      return port;
    if (uri.host() == null)
      port = Integer.parseInt(uri.toString());
    if (port > 0)
      return port;
    return defaultPort();
  }

  // Start listening on the interface.
  public final void start() {
    try {
      start0();
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  } private final void start0() throws Exception {
    ServerBootstrap sb = new ServerBootstrap();
    sb.channel(channel());
    sb.group(acceptor, new NioEventLoopGroup());
    sb.childHandler(this);

    // Set some nice options everyone likes.
    sb.option(ChannelOption.TCP_NODELAY, true);
    sb.option(ChannelOption.SO_KEEPALIVE, true);

    // Determine host and port from uri.
    InetAddress ia = InetAddress.getByName(uri.host());
    InetSocketAddress addr = new InetSocketAddress(ia, getPortFromUri());

    // Bind socket to the given host/port.
    sb.bind(addr).sync();
    Log.info("Listening for ", name, " connections on: "+addr);
  }

  // Handler to hand the request off to the scheduler.
  private class AdHandler extends ChannelInboundHandlerAdapter {
    public void channelRead(final ChannelHandlerContext ctx, Object o)
    throws Exception {
      sched.putRequest(new Request((Ad)o) {
        protected void done(Object object) {
          if (object == null)
            ctx.writeAndFlush(
              new Ad("message", "Operation completed successfully."));
          else
            ctx.writeAndFlush(Ad.marshal(object));
        } protected void fail(Throwable t) {
          t.printStackTrace();
          String m = t.getMessage();
          if (m == null) {
            String n = t.getClass().getSimpleName();
            m = "Unspecified error";
            if (!n.isEmpty()) m += " ("+n+")";
          }
          ctx.writeAndFlush(new Ad("error", m));
        }
      });
    }
  }

  // Interface wrapper around init().
  public final void initChannel(Channel ch) throws Exception {
    Log.fine(name, ": connection from ", ch);
    init(ch, ch.pipeline());
    ch.pipeline().addLast(new AdHandler());
  }

  // Override this to use a transport protocol other than TCP.
  public Class<? extends ServerChannel> channel() {
    return NioServerSocketChannel.class;
  }

  // Initialize the newly connected channel. A handler for talking to
  // the associated scheduler will be appended automatically.
  public abstract void init(Channel ch, ChannelPipeline pl);

  // Get the default port.
  public abstract int defaultPort();
}
