package stork.scheduler;

import stork.util.*;

import io.netty.bootstrap.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;

import java.util.*;
import java.nio.charset.Charset;
import java.net.InetSocketAddress;

// A bunch of classes under one namespace that implement various
// pieces needed to use Netty for network communications.

public final class NettyStuff {

  private NettyStuff() { /* Don't instantiate me! */ }

  // Codecs
  // ------
  // A decoder for parsing ads from channels.
  public static class AdDecoder extends ReplayingDecoder {
    protected Object decode(ChannelHandlerContext ctx, ByteBuf buf)
    throws Exception {
      // Feed the byte buffer to the ad parser as an input stream.
      try {
        return Ad.parse(new ByteBufInputStream(buf));
      } catch (Exception e) {
        e.printStackTrace();
        ctx.close();
        throw e;
      }
    }
  }

  // An encoder for writing ads to a channel.
  public static class AdEncoder extends MessageToByteEncoder<Ad> {
    protected void encode(ChannelHandlerContext ctx, Ad ad, ByteBuf out)
    throws Exception {
      out.writeBytes(ad.serialize());
    }
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

      System.out.println("Initing channel: "+ch.localAddress());

      pl.addLast("decoder", new AdDecoder());
      pl.addLast("encoder", new AdEncoder());
      pl.addLast(new AdServerHandler(sched));
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
        public synchronized void onRing(Ad ad) {
          ctx.write(ad);
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
      System.out.println("Bound channel...");
    }
  }
}
