package stork.module.ftp;

import java.net.*;
import java.util.*;
import java.nio.charset.*;

import io.netty.bootstrap.*;
import io.netty.channel.*;
import io.netty.buffer.*;
import io.netty.channel.nio.*;
import io.netty.channel.socket.*;
import io.netty.channel.socket.nio.*;
import io.netty.handler.codec.*;
import io.netty.handler.codec.string.*;
import io.netty.handler.codec.base64.*;
import io.netty.util.*;

import org.ietf.jgss.*;
import org.gridforum.jgss.*;

import stork.feather.*;
import stork.feather.URI;
import stork.util.*;

public class FTPDataChannel {
  // The maximum amount of time (in ms) to wait for a connection.
  private static final int timeout = 2000;

  private final Bell<Void> onClose = new Bell<Void>();

  private FTPHostPort hostport;
  private FTPChannel channel;
  private ChannelFuture future;

  public FTPDataChannel(FTPChannel ch, FTPHostPort hp) {
    hostport = hp;
    channel = ch;

    Bootstrap b = new Bootstrap();
    b.group(FTPChannel.group).channel(NioSocketChannel.class);
    b.handler(new Initializer());

    future = b.connect(hp.getAddr());
  }

  // Set up the channel pipeline.
  class Initializer extends ChannelInitializer<SocketChannel> {
    public void initChannel(SocketChannel ch) throws Exception {
      ch.config().setConnectTimeoutMillis(timeout);
      ch.pipeline().addLast(new SliceHandler());
    }
  }

  // Handle incoming data chunks and forward to handler.
  // TODO: Mode E, encryption, pause/resume.
  class SliceHandler extends ChannelHandlerAdapter {
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      receive(new Slice((ByteBuf) msg));
    }
  }

  private Channel channel() {
    return future.syncUninterruptibly().channel();
  }

  /** Subclasses use this to handle slices. */
  public void receive(Slice slice) { }

  /** Send a slice through the data channel. */
  public void send(Slice slice) {
    channel().write(slice.asByteBuf());
  }
}
