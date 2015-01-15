package stork.feather.net;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;

import stork.feather.*;

class TCPSocket extends Socket {
  private Bell<SocketAddress> addr;
  private Bell<SocketChannel> channel = new Bell<SocketChannel>();

  /** Our selector. */
  private Selector<SocketChannel> selector;

  private ByteBuffer readBuffer;
  private List<ByteBuffer> writeBuffer = new LinkedList<ByteBuffer>();

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
    return addr.new AsBell<SocketChannel>() {
      public Bell<SocketChannel> convert(SocketAddress addr) throws Exception {
        SocketChannel ch = SocketChannel.open();
        ch.configureBlocking(false);
        selector = new Selector<SocketChannel>(ch);
        Bell<SocketChannel> bell = selector.onConnectable().promise(channel);
        ch.connect(addr);
        return bell;
      }
    }.new As<SocketChannel>() {
      public SocketChannel convert(SocketChannel ch) throws Exception {
        ch.finishConnect();
        return ch;
      } public void done() {
        allocateReadBuffer();
        expectRead();
      }
    }.promise(channel);
  }

  /** Allocate a new read buffer. */
  private synchronized void allocateReadBuffer() {
    readBuffer = ByteBuffer.allocate(2048);
  }

  // Call when we're expecting to read.
  private void expectRead() {
    selector.onReadable().new Promise() {
      public void done(SocketChannel ch) { doRead(ch); }
    };
  }

  // Read into the readBuffer.
  private synchronized void doRead(SocketChannel ch) {
    int size;
    byte[] bytes;

    try {
      size = ch.read(readBuffer);
    } catch (Exception e) {
      close(e);
      return;
    }

    // There was nothing to read after all...
    if (size == 0) {
      expectRead();
      return;
    }

    // If we used the whole read buffer, just emit it.
    if (!readBuffer.hasRemaining()) {
      bytes = readBuffer.array();
      allocateReadBuffer();
    } else {
      bytes = new byte[size];
      readBuffer.position(0);
      readBuffer.get(bytes);
      readBuffer.position(0);
    }

    // Emit and don't try to read again until the pipeline is ready.
    emit(bytes).new Promise() {
      public void done() { expectRead(); }
    };
  }

  private Bell<Void> lastWrite = Bell.rungBell();

  protected synchronized void code(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    if (bytes.length > 0)
      lastWrite = lastWrite.new AsBell<Void>() {
        public Bell<Void> convert(Void _) { return write(buffer); }
      }.detach();
  }

  protected void code(Throwable error) {
    close(error);
  }

  /**
   * Write to the channel. Returns a bell that rings when all of the bytes have
   * been written to the channel.
   */
  private Bell<Void> write(final ByteBuffer bytes) {
    return channel.new AsBell<Void>() {
      public Bell<Void> convert(SocketChannel ch) throws Exception {
        ch.write(bytes);
        if (!bytes.hasRemaining())
          return Bell.rungBell();
        Bell<Void> pause = selector.onWritable().new AsBell<Void>() {
          public Bell<Void> convert(SocketChannel ch) { return write(bytes); }
        };
        pause(pause);
        return pause;
      } public void fail(Throwable t) {
        close(t);
      }
    };
  }

  protected void doClose() throws Exception {
    if (channel == null)
      return;
    if (!channel.isDone())
      channel.cancel();
    else
      channel.sync().close();
  }

  public static void main(String[] args) throws Exception {
    Coder.loop(new TCPSocket("127.0.0.1", 12345).open());
    System.in.read();
  }
}
