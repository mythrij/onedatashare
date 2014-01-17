package stork.module.ftp;

import java.net.*;
import java.util.*;
import java.nio.charset.*;

import stork.cred.*;

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

// Internal representation of the remote server type.
enum Type {
  ftp(21), gridftp(2811);

  int port;

  Type(int def_port) {
    port = def_port;
  }
}

// An abstraction of an FTP control channel.
public class FTPChannel {
  private Type type;
  private ChannelFuture future;
  private SecurityContext security;
  private Deque<Command> handlers;
  private Reply welcome;

  // Any FTP server that adheres to specifications will use UTF-8, but let's
  // not preclude the possibility of being able to configure the encoding.
  private Charset encoding = CharsetUtil.UTF_8;

  private static EventLoopGroup group = new NioEventLoopGroup();

  public FTPChannel(String uri) {
    this(URI.create(uri));
  } public FTPChannel(URI uri) {
    this(uri.getScheme(), uri.getHost(), uri.getPort());
  } public FTPChannel(String host, int port) {
    this(null, host, port);
  } public FTPChannel(InetAddress addr, int port) {
    this(null, addr, port);
  } public FTPChannel(String proto, String host, int port) {
    this(proto, host, null, port);
  } public FTPChannel(String proto, InetAddress addr, int port) {
    this(proto, null, addr, port);
  }

  // All the constructors delegate to this.
  private FTPChannel(String proto, String host, InetAddress addr, int port) {
    type = (proto == null) ? Type.ftp : Type.valueOf(proto.toLowerCase());
    if (port <= 0) port = type.port;

    Bootstrap b = new Bootstrap();
    b.group(group).channel(NioSocketChannel.class).handler(new Initializer());
    future = (host != null) ? b.connect(host, port) : b.connect(addr, port);

    handlers = new ArrayDeque<Command>();
  }

  // Handles attaching the necessary codecs.
  class Initializer extends ChannelInitializer<SocketChannel> {
    public void initChannel(SocketChannel ch) throws Exception {
      ChannelPipeline p = ch.pipeline();

      p.addLast("reply_decoder", new ReplyDecoder(20480));
      p.addLast("reply_handler", new ReplyHandler());

      p.addLast("command_encoder", new CommandEncoder());

    }
  }

  // A reply from the server.
  public class Reply {
    public final int code;
    private final ByteBuf[] lines;

    private Reply(int code, ByteBuf[] lines) {
      if (code < 100 || code >= 700)
        throw new RuntimeException("Bad reply code: "+code);
      this.code = code;
      this.lines = lines;
    }

    // Get the number of lines.
    public int lines() {
      return lines.length;
    }

    // Get a line by number.
    public String line(int i) {
      return lines[i].toString(encoding);
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0, z = lines.length-1; i <= z; i++) if (i != z)
        sb.append(code).append('-').append(line(i)).append('\n');
      else
        sb.append(code).append(' ').append(line(i));
      return sb.toString();
    }

    // Returns true if this reply is intermediate and further replies are
    // expected before the command has been fulfilled.
    public boolean isPreliminary() {
      return code/100 == 1;
    }

    // Returns true if the reply indicates the command completed successfully.
    public boolean isComplete() {
      return code/100 == 2;
    }

    // Returns true if the reply indicates the command cannot be carried out
    // unless followed up with another command.
    public boolean isIncomplete() {
      return code/100 == 3;
    }

    // Returns true if the reply is negative (i.e., it indicates a failure).
    public boolean isNegative() {
      return code/100 == 4 || code/100 == 5;
    }

    // Returns true if the message is protected, i.e. its message is a payload
    // containing a specially encoded reply. In most cases, users will not see
    // replies of this type directly as the channel handlers will transparently
    // decode them. However, this can happen if server behaves improperly by
    // sending replies of this type with no security context set.
    public boolean isProtected() {
      return code/100 == 6;
    }
  }

  // Handles decoding replies from the server. This uses functionality from
  // LineBasedFrameDecoder to split incoming bytes on a per-line basis, then
  // parses each line as an FTP reply line, and buffers lines until a reply is
  // fully formed. It supports decoding (arbitrarily-nested) protected replies
  // when a security context is present.
  class ReplyDecoder extends LineBasedFrameDecoder {
    List<ByteBuf> lines = new LinkedList<ByteBuf>();
    String codestr;
    int code;

    public ReplyDecoder(int len) {
      super(len);
    }

    // Convenience method for parsing reply text in a self-contained byte
    // buffer, used by decodeProtectReply and anywhere else this might be
    // needed. This works by simulating incoming data in a channel pipeline,
    // though no channel context is present. This may be an issue if Netty ever
    // changes to use the channel context in line splitting, in which case we
    // will need to come up with another solution.
    private Reply decodeProtectedReply(Reply reply) throws Exception {
      // Do nothing if the security context has not been initialized. Really
      // this is an error on the part of the server, but there's a chance the
      // client code will know what to do.
      if (security == null)
        return reply;

      ReplyDecoder rd = new ReplyDecoder(20480);
      for (ByteBuf eb : reply.lines) {
        ByteBuf db = security.unprotect(Base64.decode(eb));
        Reply r = rd.decode(null, db);
        if (r != null)
          return r;
      } throw new RuntimeException("Bad reply from server.");
    }

    // Override the decode from LineBasedFrameDecoder to feed incoming lines to
    // decodeReplyLine.
    protected Reply decode(ChannelHandlerContext ctx, ByteBuf buffer)
    throws Exception {
      buffer = (ByteBuf) super.decode(ctx, buffer);
      if (buffer == null)
        return null;  // We haven't gotten a full line.
      Reply r = decodeReplyLine(buffer);
      if (r == null)
        return null;  // We haven't gotten a full reply.
      if (r.isProtected())
        r = decodeProtectedReply(r);
      return r;
    }

    // Decode a reply from a string. This should only be called if the input
    // buffer is a properly framed line with the end-of-line character(s)
    // stripped.
    protected Reply decodeReplyLine(ByteBuf msg) {
      try {
        return innerDecodeReplyLine(msg);
      } catch (Exception e) {
        // TODO: Replace this with a real exception.
        throw new RuntimeException("Bad reply from server.");
      }
    } protected Reply innerDecodeReplyLine(ByteBuf msg) throws Exception {
      // Some implementation supposedly inserts null bytes, ugh.
      if (msg.getByte(0) == 0)
        msg.readByte();

      char sep = '-';

      // Extract the reply code and separator.
      if (lines.isEmpty()) {
        codestr = msg.toString(0, 3, encoding);
        code = Integer.parseInt(codestr);
        msg.skipBytes(3);
        sep = (char) msg.readByte();
      } else if (msg.readableBytes() >= 4) {
        String s = msg.toString(0, 4, encoding);
        sep = s.charAt(3);
        if (s.startsWith(codestr) && (sep == '-' || sep == ' '))
          msg.skipBytes(4);
        else
          sep = '-';
      }

      // Save the rest of the message.
      lines.add(msg);

      // Act based on the separator.
      switch (sep) {
        case ' ': return emitReply(code);
        case '-': return null;
        default : throw  null;
      }
    }

    // Emit the reply we've been buffering and reset the buffer.
    private Reply emitReply(int code) {
      ByteBuf[] la = lines.toArray(new ByteBuf[lines.size()]);
      lines = new LinkedList<ByteBuf>();
      return new Reply(code, la);
    }
  }

  // Handles replies as they are received.
  class ReplyHandler extends SimpleChannelInboundHandler<Reply> {
    public void messageReceived(ChannelHandlerContext ctx, Reply msg)
    throws Exception {
      System.out.println(msg);
      if (welcome == null)
        welcome = msg;
      else
        feedHandler(msg);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
      t.printStackTrace();
    }
  }

  // A security context represents all of the state of the channel with respect
  // to security and provides methods for encoding and decoding messages.
  interface SecurityContext {
    // Returns true if the session has been established.
    boolean established();

    // Given an input token, return an output token which should be given to
    // the remote server via ADAT, or null if the session is established.
    ByteBuf handshake(ByteBuf in) throws Exception;

    // Given a byte buffer containing protected bytes (not a Base64 encoding
    // thereof), decode the bytes back into the plaintext payload.
    ByteBuf unprotect(ByteBuf buf) throws Exception;

    // Given a byte buffer containing some information, generate a command with
    // a payload protected according to the current security context.
    ByteBuf protect(ByteBuf buf) throws Exception;
  }

  // A security context based on GSSAPI.
  class GSSSecurityContext implements SecurityContext {
    GSSContext context;

    public GSSSecurityContext(GSSCredential cred) throws GSSException {
      GSSManager manager = ExtendedGSSManager.getInstance();
      Oid oid = new Oid("1.3.6.1.4.1.3536.1.1");
      String host = "trestles-dm.sdsc.xsede.org";
      GSSName peer = manager.createName(
        "host@"+host, GSSName.NT_HOSTBASED_SERVICE);
      context = manager.createContext(
        peer, oid, cred, cred.getRemainingLifetime());
      context.requestCredDeleg(true);
      context.requestConf(true);
    }

    // Utility method for extracting a buffer as a byte array.
    private byte[] bytes(ByteBuf buf) {
      byte[] b;
      if (buf.hasArray())
        b = buf.array();
      else
        buf.getBytes(buf.readerIndex(), b = new byte[buf.readableBytes()]);
      return b;
    }

    public boolean established() {
      return context.isEstablished();
    }

    public ByteBuf handshake(ByteBuf in) throws GSSException {
      if (established())
        return null;
      byte[] i = bytes(in);
      byte[] o = context.initSecContext(i, 0, i.length);
      return Unpooled.wrappedBuffer(o);
    }

    public ByteBuf unprotect(ByteBuf buf) throws GSSException {
      byte[] b = bytes(buf);
      return Unpooled.wrappedBuffer(context.unwrap(b, 0, b.length, null));
    }

    public ByteBuf protect(ByteBuf buf) throws GSSException {
      byte[] b = bytes(buf);
      return Unpooled.wrappedBuffer(context.wrap(b, 0, b.length, null));
    }
  }

  // Handles encoding commands using the current charset and security
  // mechanism. We can assume every message is a single command with no
  // newlines.
  class CommandEncoder extends MessageToMessageEncoder<Object> {
    protected void encode
      (ChannelHandlerContext ctx, Object msg, List<Object> out)
    throws Exception {
      System.out.println(msg);
      ByteBuf b = Unpooled.wrappedBuffer(msg.toString().getBytes(encoding));

      if (security != null) {
        b = Base64.encode(security.protect(b), false);
        out.add(Unpooled.wrappedBuffer("ENC ".getBytes(encoding)));
      } 

      out.add(b);
      out.add(Unpooled.wrappedBuffer("\r\n".getBytes(encoding)));
    }
  }

  // Used internally to extract the channel from the future.
  private Channel channel() {
    return future.syncUninterruptibly().channel();
  }

  // Try to authenticate using a username and password.
  public void authorize() {
    authorize("anonymous", "");
  } public void authorize(String user) {
    authorize(user, "");
  } public void authorize(final String user, final String pass) {
    new Command("USER", user) {
      public void handle(Reply r) {
        if (r.isIncomplete())
          new Command("PASS", pass);
      }
    }.sync();
  }

  // Try to authenticate with a GSS credential.
  public void authenticate(GSSCredential cred) throws Exception {
    final GSSSecurityContext sec = new GSSSecurityContext(cred);

    new Command("AUTH GSSAPI") {
      public void handle(Reply r) {
        if (r.isIncomplete())
          handshake(sec, Unpooled.EMPTY_BUFFER);
        else if (r.isComplete())
          security = sec;
      }
    }.sync();
  }

  // Handshake procedure for all authentication types. The input byte buffer
  // should be the raw binary token, not a Base64 encoding.
  private void handshake(final SecurityContext sec, ByteBuf it) {
    try {
      ByteBuf ot = Base64.encode(sec.handshake(it), false);

      new Command("ADAT", ot.toString(encoding)) {
        public void handle(Reply r) throws Exception {
          if (r.isIncomplete()) {
            ByteBuf token = Base64.decode(r.lines[0].skipBytes(5));
            handshake(sec, token);
          }
        }
      };
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Append the given command to the handler queue. If the command is a sync
  // command and there is nothing else in the queue, just fulfill it
  // immediately and don't put it in the queue. This will return whether the
  // command handler was appended or not (currently, it is always true).
  private boolean appendHandler(Command c) {
    synchronized (handlers) {
      if (!c.isSync() || !handlers.isEmpty())
        return handlers.add(c);
    }

    // If we didn't return, it's a sync. Handling a sync is fast, but we should
    // release handlers as soon as possible.
    c.internalHandle(null);
    return true;
  }

  // "Feed" the topmost handler a reply. If the reply is a preliminary reply,
  // it will pop the command handler and any sync commands between it and the
  // next non-sync command (or the end of the queue). In order to release the
  // monitor on the queue as soon as possible, the command handlers are not
  // called until after the queue has been modified.
  private void feedHandler(Reply reply) {
    Command handler;
    List<Command> syncs = null;

    // We should try to release this as soon as possibile, so don't do anything
    // that might take a long time in here.
    synchronized (handlers) {
      assert !handlers.isEmpty();

      if (reply.isPreliminary()) {
        handler = handlers.peek();
      } else {
        handler = handlers.pop();

        // Remove all the syncs.
        Command peek = handlers.peek();
        if (peek != null && peek.isSync()) {
          syncs = new LinkedList<Command>();
          do {
            syncs.add(peek);
            handlers.pop();
            peek = handlers.peek();
          } while (peek != null && peek.isSync());
        }
      }
    }

    // Now we can call the handlers.
    handler.internalHandle(reply);
    if (syncs != null) for (Command sync : syncs)
      sync.internalHandle(null);
  }

  // Used internally to write commands to the channel.
  private synchronized void writeCmd(final Object cmd, final Object... more) {
    // Write a small object which will flatten the passed objects when it's
    // ready to be written.
    channel().writeAndFlush(new Object() {
      public String toString() {
        StringBuilder sb = new StringBuilder(cmd.toString());
        for (Object o : more)
          sb.append(" ").append(o);
        return sb.toString();
      }
    });
  }

  // This is sort of the workhorse of the channel. Instantiate one of these to
  // send a command across this channel. The newly instantiated object will
  // serve as a "future" for the server's ultimate reply. For example, a simple
  // authorization routine could be written like:
  //
  //   FTPChannel ch = new FTPChannel(...);
  //   Reply r = ch.new Command("USER bwross") {
  //     public void handle(Reply r) {
  //       if (r.code == 331)
  //         new Command("PASS monkey");
  //     }
  //   }.sync();
  //
  // This class's constructor will cause the command to be written to the
  // channel and placed in the handler queue. This class can be anonymously
  // subclassed to write command handlers inline, or can be subclassed normally
  // for repeat issue of the command.
  public class Command {
    // XXX: Something a little hacky here to minimize storage overhead.
    // Normally, reply is either null (to indicate the reply is pending) or a
    // Reply object (the ultimate reply for the command). Sync commands are a
    // special case. An unfulfilled sync command sets reply to Command.class,
    // and a fulfilled sync command sets reply to Command.this. It's a little
    // weird, I know, but at any given time there may be a large number of
    // commands in the pipeline, so reducing object overhead even a little is
    // beneficial.
    private Object reply;

    // Constructing this will automatically cause the given command to be
    // written to the server. Typically, the passed cmd will be a string, but
    // can be anything. The passed arguments will be stringified and
    // concatenated with spaces for convenience. If cmd is null, nothing is
    // actually written to the server and the command serves as a sort of
    // "sync" for client code. Calling sync() on it will block until it has
    // been reached in the pipeline.
    public Command(Object cmd, Object... more) {
      synchronized (FTPChannel.this) {
        if (cmd == null)  // See the comment at the top.
          reply = Command.class;
        appendHandler(this);
        if (cmd != null)
          writeCmd(cmd, (Object[]) more);
      }
    }

    // See the comment at the top of the class definition for why these are
    // implemented the way they are.
    public synchronized final boolean isSync() {
      return !(reply == null || reply instanceof Reply);
    } public synchronized final boolean isDone() {
      return !(reply == null || reply == Command.class);
    }

    // This is called internally by the channel handler whenever a reply comes
    // in. It handles passing the received reply to handle(). Once this is
    // passed a non-preliminary reply, it sets the reply future
    private void internalHandle(Reply r) {
      try {
        synchronized (this) { handle(r); }
      } catch (Exception e) {
        // Just discard handler exceptions.
      } finally {
        if (isSync())
          resolve(null);
        else if (!r.isPreliminary())
          resolve(r);
      }
    }

    // Call this only when this command's ultimate reply has been received. It
    // will wake up any threads syncing on this command.
    private synchronized void resolve(Reply r) {
      // See the comment at the top of the class definition.
      reply = isSync() ? Command.this : r;
      notifyAll();
    }

    // Users should subclass this and override handle() to deal with commands
    // as they arrive. This method may be called multiple times if preliminary
    // replies are received. A non-preliminary reply indicates that the handler
    // will not be called again. If an exception is thrown here, it will be
    // silently ignored. Note that the code that calls this is synchronized so that
    // it will be called in the order replies are received and 
    public void handle() throws Exception {
      // The default implementation does nothing.
    } public void handle(Reply r) throws Exception {
      handle();
    }

    // This is used to wait until the future has been resolved. The ultimate
    // reply will be returned, unless this was a sync command, in which case
    // null will be returned.
    public synchronized Reply sync() {
      while (!isDone()) try {
        wait();
      } catch (InterruptedException e) {
        // Ugh, let's just handle it like this for now...
        throw new RuntimeException(e);
      } return isSync() ? null : (Reply) reply;
    }
  }

  public static void main(String[] args) throws Exception {
    FTPChannel ch = new FTPChannel("ftp://didclab-ws8/");
    ch.authorize();
    //GSSCredential cred =
      //StorkGSSCred.fromFile("/tmp/x509up_u1000").credential();
    //ch.authenticate(cred);
    ch.new Command("MLSC /").sync();
  }
}
