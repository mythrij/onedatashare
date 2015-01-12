package stork.feather.net;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import stork.feather.*;

class TCPSocket extends Socket {
  private Bell<SocketAddress> addr;
  private Bell<SocketChannel> channel = new Bell<SocketChannel>() {
    public void done() { expectRead(); }
  };

  /** Our selector. */
  private Selector<SocketChannel> selector;

  private ByteBuffer readBuffer;
  private List<ByteBuffer> writeBuffer = new LinkedList<ByteBuffer>();

  /** This will be set if we're waiting to write. */
  private Bell<?> expectWrite;

  public TCPSocket(String host, int port) {
    this(DNSResolver.resolve(host), port);
  }

  public TCPSocket(InetAddress host, int port) {
    this(new InetSocketAddress(host, port));
  }

  public TCPSocket(Bell<InetAddress> host, final int port) {
    addr = host.new As<SocketAddress>() {
      public SocketAddress convert(InetAddress addr) {
        return new InetSocketAddress(addr, port);
      }
    };
  }

  public TCPSocket(SocketAddress addr) {
    this.addr = Bell.wrap(addr);
  }

  public TCPSocket(Bell<SocketAddress> addr) {
    this.addr = addr;
  }

  protected Bell<?> doOpen() {
    addr.new Promise() {
      public void done(SocketAddress addr) {
        connect(addr);
      } public void fail(Throwable t) {
        channel.ring(t);
      }
    };
    return channel;
  }

  private void connect(SocketAddress addr) {
    try {
      SocketChannel ch = SocketChannel.open();
      ch.configureBlocking(false);
      selector = new Selector<SocketChannel>(ch);
      selector.onConnect().new As<SocketChannel>() {
        public SocketChannel convert(SocketChannel ch) throws Exception {
          ch.finishConnect();
          return ch;
        }
      }.promise(channel);
      readBuffer = ByteBuffer.allocateDirect(2048);
      ch.connect(addr);
    } catch (Exception e) {
      e.printStackTrace();
      channel.ring(e);
    }
  }

  // Call when we're expecting to read.
  private void expectRead() {
    Bell<?> b = selector.onRead();
    selector.onRead().new Promise() {
      public void done(SocketChannel ch) { doRead(ch); }
    };
  }

  // Read into the readBuffer.
  private synchronized void doRead(SocketChannel ch) {
    int size;

    try {
      size = ch.read(readBuffer);
    } catch (Exception e) {
      close(e);
      return;
    }

    if (size == 0) {
      expectRead();
      return;
    }

    byte[] bytes = new byte[size];

    readBuffer.position(0);
    readBuffer.get(bytes);
    readBuffer.position(0);

    emit(bytes).new Promise() {
      public void done() { expectRead(); }
    };
  }

  protected void handle(final byte[] bytes) {
    if (bytes.length == 0)
      return;
    channel.new Promise() {
      public void done(SocketChannel ch) throws Exception {
        System.out.println("Writing bytes: \""+new String(bytes)+"\"");
        writeBuffer.add(ByteBuffer.wrap(bytes));
        expectWrite();
      }
    };
  }

  // Call when we're expecting to write.
  private synchronized void expectWrite() {
    if (expectWrite == null)
      expectWrite = selector.onWrite().new Promise() {
        public void done(SocketChannel ch) { doWrite(ch); }
      };
  }

  private synchronized void doWrite(SocketChannel ch) {
    while (writeBuffer.size() > 0) try {
      ByteBuffer buf = writeBuffer.remove(0);
      ch.write(buf);
      if (buf.hasRemaining()) {
        writeBuffer.add(0, buf);
        expectWrite();
        return;
      }
    } catch (Exception e) {
      close(e);
      return;
    }
  }

  protected void doClose() throws Exception {
    if (channel == null)
      return;
    if (!channel.isDone())
      channel.cancel();
    else
      channel.sync().close();
  }

  public static void main(String[] args) {
    final Socket s = new TCPSocket("stream3.gameowls.com", 8000);
    //final Socket s = new TCPSocket("google.com", 8000);
    //final Socket s = new TCPSocket("127.0.0.1", 12345);

    //s.feed("GET /images/srpr/logo11w.png\r\n".getBytes());
    s.feed("GET /chiptune.ogg HTTP/1.1\r\n\r\n".getBytes());

    Codec<?,?> c = s.open().join(new Codec<byte[],String>("thing") {
      public void handle(byte[] bytes) {
        System.out.println("Got bytes: "+bytes.length);
        System.out.println(new Slice(bytes));
      } public void handle(Throwable t) {
        System.out.println("Got error: "+t);
      }
    });
    s.onClose().sync();
  }
}
