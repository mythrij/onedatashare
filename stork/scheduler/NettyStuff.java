package stork.scheduler;

import stork.util.*;
import stork.ad.*;
import stork.*;

import io.netty.bootstrap.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;

import java.io.*;
import java.net.*;

// A bunch of classes under one namespace that implement various
// pieces needed to use Netty for network communications.

public abstract class NettyStuff {
  // Codecs
  // ------
  // A decoder for parsing ads from channels.
  public static class AdCodec {
    AdPrinter printer = AdPrinter.CLASSAD;

    // A decoder for reading ads from a channel.
    ReplayingDecoder<Ad> decoder = new ReplayingDecoder<Ad>() {
      protected Ad decode(ChannelHandlerContext ctx, final ByteBuf buf)
      throws Exception {
        boolean close = true;
        try {
          if (actualReadableBytes() == 0)
            return null;

          Ad ad = Ad.parse(new InputStream() {
            int i = 0, len = actualReadableBytes();
            public int read() {
              return (i++ == len) ? -1 : buf.readByte();
            }
          });

          Log.finer("Got ad: ", ad);

          if (ad instanceof AdParser.ParsedAd)
            AdCodec.this.printer = ((AdParser.ParsedAd)ad).printer;

          return ad;
        } catch (Exception e) {
          // We got something nasty.
          ctx.write(new Ad("error", e.getMessage()));
          ctx.close();
          throw e;
        }
      }
    };

    // An encoder for writing ads to a channel. Uses the encapsulated printer.
    MessageToByteEncoder<Ad> encoder = new MessageToByteEncoder<Ad>() {
      protected void encode(ChannelHandlerContext ctx, Ad ad, ByteBuf out)
      throws Exception {
        Log.finer("Writing ad: ", ad);
        out.writeBytes(printer.toString(ad).getBytes("UTF-8"));
        ctx.flush();
      }
    };
  }

  // Handlers
  // --------
  // Move ads between scheduler and client connection.
  public static class AdServerHandler
  extends ChannelInboundMessageHandlerAdapter<Ad> {
    StorkScheduler sched;

    public AdServerHandler(StorkScheduler sched) {
      this.sched = sched;
    }

    public void messageReceived(final ChannelHandlerContext ctx, Ad ad) {
      // Hand the request off to the scheduler.
      sched.putRequest(ad, new Bell<Ad>() {
        public synchronized void onRing(Ad ad) {
          if (ad != null)
            ctx.write(ad);
          ctx.flush();
        }
      });
    }
  }

  // Scheduler Interfaces
  // --------------------
  // Automatically determine and create an interface from a URI.
  public static StorkInterface createInterface(StorkScheduler s, URI u)
  throws Exception {
    Log.fine("Making interface for ", u, "...");
    String p = u.getScheme();
    if (p == null)
      p = u.toString();
    if (p.equals("tcp"))
      return new TcpInterface(s, u);
    if (p.equals("http"))
      return new HttpInterface(s, u);
    if (p.equals("https"))
      return new HttpInterface(s, u);
    throw new RuntimeException("unknown interface scheme: "+p);
  }

  // This doesn't do anything right now.
  public static interface StorkInterface { }
  
  // Basic TCP interface.
  public static class TcpInterface implements StorkInterface {
    static final int DEFAULT_PORT = 57024;
    public TcpInterface(final StorkScheduler s, URI uri) throws Exception {
      ServerBootstrap sb = new ServerBootstrap();
      sb.channel(NioServerSocketChannel.class);
      sb.group(new NioEventLoopGroup(), new NioEventLoopGroup());
      sb.childHandler(new ChannelInitializer<SocketChannel>() {
        public void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline pl = ch.pipeline();

          AdCodec codec = new AdCodec();
          pl.addLast("decoder", codec.decoder);
          pl.addLast("encoder", codec.encoder);
          pl.addLast(new AdServerHandler(s));

          // Write welcome ad.
          ch.write(new Ad("host", ch.localAddress().getHostName())
                     .put("name", "Stork")
                     .put("version", Stork.version()));
        }
      });

      // Set some nice options.
      sb.option(ChannelOption.TCP_NODELAY, true);
      sb.option(ChannelOption.SO_KEEPALIVE, true);

      // Determine host and port from uri.
      InetAddress ia = InetAddress.getByName(uri.getHost());
      int port = (uri.getPort() > 0) ? uri.getPort() : DEFAULT_PORT;
      InetSocketAddress addr = new InetSocketAddress(ia, port);

      // Bind socket to the given host/port.
      sb.bind(addr).sync();
      Log.info("Listening for TCP connections on: "+addr);
    }
  }

  // Basic REST/HTTP interface.
  // TODO: I guess we gotta worry about certificate management huh.
  public static class HttpInterface implements StorkInterface {
    public HttpInterface(final StorkScheduler s, URI uri) throws Exception {
      // Check whether we're using HTTP or HTTPS.
      final boolean https = "https".equals(uri.getScheme());
      int port = https ? 443 : 80;

      Log.warning("The HTTP interface does not work yet.");

      ServerBootstrap sb = new ServerBootstrap();
      sb.channel(NioServerSocketChannel.class);
      sb.group(new NioEventLoopGroup(), new NioEventLoopGroup());
      sb.childHandler(new ChannelInitializer<SocketChannel>() {
        public void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline pl = ch.pipeline();

          // TODO: SSL engine.

          pl.addLast("decoder", new HttpRequestDecoder());
          pl.addLast("encoder", new HttpResponseEncoder());
          pl.addLast("deflater", new HttpContentCompressor());
          //pl.addLast(new HttpAdHandler(s));
          pl.addLast(new AdServerHandler(s));
        }
      });

      // Set some nice options.
      sb.option(ChannelOption.TCP_NODELAY, true);
      sb.option(ChannelOption.SO_KEEPALIVE, true);

      // Determine host and port from uri.
      InetAddress ia = InetAddress.getByName(uri.getHost());
      port = (uri.getPort() > 0) ? uri.getPort() : port;
      InetSocketAddress addr = new InetSocketAddress(ia, port);

      // Bind socket to the given host/port.
      sb.bind(addr).sync();
      Log.info("Listening for HTTP requests on: "+addr);
    }
  }
}
