package stork.scheduler;

import stork.util.*;
import stork.ad.*;
import stork.*;

import io.netty.bootstrap.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;
import static io.netty.util.CharsetUtil.UTF_8;

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

    ReplayingDecoder<Ad> decoder = new ReplayingDecoder<Ad>() {
      protected Ad decode(ChannelHandlerContext ctx, final ByteBuf buf)
      throws Exception {
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
          e.printStackTrace();
          ctx.close();
          throw e;
        } catch (Error e) {
          e.printStackTrace();
          throw e;
        } finally {
          //ctx.close();
        }
      }
    };

    // An encoder for writing ads to a channel. Uses the encapsulated printer.
    MessageToByteEncoder<Ad> encoder = new MessageToByteEncoder<Ad>() {
      protected void encode(ChannelHandlerContext ctx, Ad ad, ByteBuf out)
      throws Exception {
        Log.finer("Writing ad: ", ad);
        out.writeBytes(AdCodec.this.printer.toString(ad).getBytes(UTF_8));
      }
    };
  }

  // Initializers
  // ------------
  // Set up an ad server.
  public static class AdServerInitializer
  extends ChannelInitializer<SocketChannel> {
    StorkScheduler sched;

    public AdServerInitializer(StorkScheduler sched) {
      this.sched = sched;
    }

    public void initChannel(SocketChannel ch) throws Exception {
      ChannelPipeline pl = ch.pipeline();

      AdCodec codec = new AdCodec();
      pl.addLast("decoder", codec.decoder);
      pl.addLast("encoder", codec.encoder);
      pl.addLast(new AdServerHandler(sched));

      // Write welcome ad.
      ch.write(new Ad("host", ch.localAddress().getHostName())
                 .put("name", "Stork")
                 .put("version", StorkMain.version()));
    }
  }

  // Handlers
  // --------
  // Handle receiving/sending ads.
  public static class AdServerHandler
  extends ChannelInboundMessageHandlerAdapter<Ad> {
    StorkScheduler sched;

    public AdServerHandler(StorkScheduler sched) {
      this.sched = sched;
    }

    public void messageReceived(final ChannelHandlerContext ctx, Ad ad) {
      // Hand the request off to the scheduler.
      sched.putRequest(ad, new Bell<Ad>() {
        // Bell rung on reply.
        public synchronized void onRing(Ad ad) {
          if (ad != null) ctx.write(ad);
        }
      }, new Bell<Ad>() {
        // Bell rung on end of request with status ad.
        public synchronized void onRing(Ad ad) {
          if (ad != null) ctx.write(ad);
        }
      });
    }
  }

  // Scheduler Interfaces
  // --------------------
  public static class TcpInterface {
    private StorkScheduler sched = null;
    private String host;
    private int port;
    
    public TcpInterface(StorkScheduler sched, String host, int port)
    throws Exception {
      ServerBootstrap sb = new ServerBootstrap();
      sb.channel(NioServerSocketChannel.class);
      sb.group(new NioEventLoopGroup(), new NioEventLoopGroup());
      sb.childHandler(new AdServerInitializer(sched));

      sb.option(ChannelOption.TCP_NODELAY, true);
      sb.option(ChannelOption.SO_KEEPALIVE, true);

      if (host == null) host = "127.0.0.1";
      this.host = host; this.port = port;
      InetSocketAddress addr = new InetSocketAddress(host, port);

      // Bind socket to the given host/port.
      sb.bind(addr).sync();
      Log.info("Listening for TCP connections on: "+addr);
    }
  }
}
