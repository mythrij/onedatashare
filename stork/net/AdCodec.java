package stork.net;

import stork.util.*;
import stork.ad.*;
import stork.*;

import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.channel.*;

import java.io.*;
import java.util.*;

// A codec bridging ads and byte streams.

public class AdCodec extends ChannelHandlerAppender {
  AdPrinter printer;

  private void setPrinter(AdPrinter p) {
    printer = p;
  }

  public AdCodec() {
    add(new ReplayingDecoder<Void>() {
      // A decoder for reading serialized ads from a byte channel.
      // TODO: We should use a smarter method of finding message ends.
      protected void decode(
          ChannelHandlerContext ctx, final ByteBuf buf, List<Object> out
      ) throws Exception {
        if (actualReadableBytes() == 0)
          return;

        Ad ad = Ad.parse(new InputStream() {
          int i = 0, len = actualReadableBytes();
          public int read() {
            return (i++ == len) ? -1 : buf.readByte();
          }
        });

        Log.finer("Got ad: ", ad);

        if (ad instanceof AdParser.ParsedAd)
          setPrinter(((AdParser.ParsedAd)ad).printer);

        out.add(ad);
      }

      public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
        // We got something nasty.
        t.printStackTrace();
        ctx.write(new Ad("error", t.getMessage()));
        ctx.close();
      }
    }, new MessageToByteEncoder<Ad>() {
      // An encoder for writing ads to a channel. Uses the encapsulated printer.
      protected void encode(ChannelHandlerContext ctx, Ad ad, ByteBuf out)
      throws Exception {
        Log.finer("Writing ad: ", ad);
        out.writeBytes(printer.toString(ad).getBytes("UTF-8"));
        ctx.writeAndFlush(out);
      }
    });

    printer = AdPrinter.CLASSAD;
  }
}
