package stork.net;

import stork.*;
import stork.ad.*;
import stork.scheduler.*;

import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;

import java.net.*;

// Basic TCP interface.

public class TcpInterface extends StorkInterface {
  public TcpInterface(StorkScheduler s, URI uri) {
    super(s, uri);
    name = "TCP";
  }

  public void init(Channel ch, ChannelPipeline pl) {
    pl.addLast(new AdCodec());
  }

  public int defaultPort() {
    return 57024;
  }
}
