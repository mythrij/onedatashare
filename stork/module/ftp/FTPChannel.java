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

// An abstraction of an FTP control channel. This class takes care of command
// pipelining, extracting replies, and maintaining channel state. Right now,
// the channel is mostly concurrency-safe, though issues could arise if
// arbitrary commands entered the pipeline during a command sequence. A
// simplisitic channel locking mechanism exists which can be used for
// synchronized access to the channel, but it is still ill-advised to have more
// than one subsystem issuing commands through the same channel at the same
// time.

public class FTPChannel {
  // The maximum amount of time (in ms) to wait for a connection.
  private static final int timeout = 2000;

  private boolean closed = false;

  // FIXME: We should use something system-wide.
  private static EventLoopGroup group = new NioEventLoopGroup();

  // Used for GSS authentication.
  private final String host;

  // Internal representation of the remote server type.
  private static enum Protocol {
    ftp(21), gridftp(2811);

    int port;

    Protocol(int def_port) {
      port = def_port;
    }
  }

  // Doing this allows state to more easily be shared between views of the same
  // channel.
  FTPSharedChannelState data;

  class FTPSharedChannelState {
    Protocol protocol;
    ChannelFuture future;
    SecurityContext security;
    Deque<Command> handlers = new ArrayDeque<Command>();

    FTPChannel owner;  // The view that owns the underlying channel.

    // Any FTP server that adheres to specifications will use UTF-8, but let's
    // not preclude the possibility of being able to configure the encoding.
    Charset encoding = CharsetUtil.UTF_8;

    Reply welcome;
    FeatureSet features = new FeatureSet();

    // Transfer type and mode. See the explanation above mode() and type() for
    // why these are bells.
    Bell<Character> mode = new Bell<Character>().ring('S');
    Bell<Character> type = new Bell<Character>().ring('A');
  }

  // Deferred commands
  Deque<Deferred> deferred = new ArrayDeque<Deferred>();

  public FTPChannel(String uri) {
    this(URI.create(uri));
  } public FTPChannel(URI uri) {
    this(uri.protocol(), uri.host(), null, uri.port());
  } public FTPChannel(String host, int port) {
    this(null, host, null, port);
  } public FTPChannel(InetAddress addr, int port) {
    this(null, null, addr, port);
  } public FTPChannel(String proto, String host, int port) {
    this(proto, host, null, port);
  } public FTPChannel(String proto, InetAddress addr, int port) {
    this(proto, null, addr, port);
  }

  // The above constructors delegate to this.
  private FTPChannel(String proto, String host, InetAddress addr, int port) {
    data = new FTPSharedChannelState();
    data.owner = this;

    this.host = (host != null) ? host : addr.toString();

    data.protocol = (proto == null) ?
      Protocol.ftp : Protocol.valueOf(proto.toLowerCase());
    if (port <= 0) port = data.protocol.port;

    Bootstrap b = new Bootstrap();
    b.group(group).channel(NioSocketChannel.class).handler(new Initializer());

    if (host != null)
      data.future = b.connect(host, port);
    else
      data.future = b.connect(addr, port);
  }

  // This special constructor is used internally to create channel views.
  // Calling this constructor will enqueue a synchronization command that will
  // allow the channel to assume control when it is reached.
  private FTPChannel(FTPChannel parent) {
    data = parent.data;
  }

  // Handles initializing the control channel connection and attaching the
  // necessary codecs.
  class Initializer extends ChannelInitializer<SocketChannel> {
    public void initChannel(SocketChannel ch) throws Exception {
      ch.config().setConnectTimeoutMillis(timeout);

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
    public int length() {
      return lines.length;
    }

    // Get a line by number.
    public String line(int i) {
      return lines[i].toString(data.encoding);
    }

    // Get an array of all the lines as strings.
    public String[] lines() {
      String[] arr = new String[lines.length];
      for (int i = 0; i < lines.length; i++)
        arr[i] = line(i);
      return arr;
    }

    // Return a description meaningful to a human based on a reply code.
    public String description() {
      return FTPMessages.fromCode(code);
    }

    // Return an exception based on the reply.
    public RuntimeException asError() {
      return new RuntimeException(description());
    }

    // Get the message as a string. If the reply is one line, the message is
    // the first line. If it's multi-lined, the message is the part between the
    // first and last lines.
    public String message() {
      if (lines.length <= 1)
        return line(0);
      StringBuilder sb = new StringBuilder();
      for (int i = 1, z = lines.length-1; i < z; i++) {
        sb.append(line(i));
        if (i != z) sb.append('\n');
      }
      return sb.toString();
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
      if (data.security == null)
        return reply;

      ReplyDecoder rd = new ReplyDecoder(20480);
      for (ByteBuf eb : reply.lines) {
        ByteBuf db = data.security.unprotect(Base64.decode(eb));
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
        codestr = msg.toString(0, 3, data.encoding);
        code = Integer.parseInt(codestr);
        msg.skipBytes(3);
        sep = (char) msg.readByte();
      } else if (msg.readableBytes() >= 4) {
        String s = msg.toString(0, 4, data.encoding);
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
        case ' ':
          ByteBuf[] la = lines.toArray(new ByteBuf[lines.size()]);
          lines = new LinkedList<ByteBuf>();
          return new Reply(code, la);
        case '-': return null;
        default : throw  null;
      }
    }
  }

  // Handles replies as they are received.
  class ReplyHandler extends SimpleChannelInboundHandler<Reply> {
    public void messageReceived(ChannelHandlerContext ctx, Reply reply) {
      Log.finer(this+": Got: "+reply);
      switch (reply.code) {
        case 220:
          if (data.welcome == null)
            data.welcome = reply;
          break;
        case 421:
          FTPChannel.this.close();
          break;
        default:
          feedHandler(reply);
      }
    }
  
    public void channelInactive(ChannelHandlerContext ctx) {
      FTPChannel.this.close();
    }

    // TODO: How should we handle exceptions? Which exceptions can this thing
    // receive anyway?
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
      Log.finer("Writing "+msg);
      ByteBuf b =
        Unpooled.wrappedBuffer(msg.toString().getBytes(data.encoding));

      if (data.security != null) {
        b = Base64.encode(data.security.protect(b), false);
        out.add(Unpooled.wrappedBuffer("ENC ".getBytes(data.encoding)));
      } 

      out.add(b);
      out.add(Unpooled.wrappedBuffer("\r\n".getBytes(data.encoding)));
    }
  }

  // Used internally to extract the channel from the future.
  private Channel channel() {
    return data.future.syncUninterruptibly().channel();
  }

  // Close the channel and run the onClose handler.
  public synchronized void close() {
    if (closed) return;
    closed = true;
    channel().close();
    onClose();
  }

  // This gets called when the channel is closed by some means. Subclasses
  // should implement this.
  protected void onClose() { }

  // Try to authenticate using a username and password.
  public Bell<Reply> authorize() {
    return authorize("anonymous", "");
  } public Bell<Reply> authorize(String user) {
    return authorize(user, "");
  } public Bell<Reply> authorize(final String user, final String pass) {
    final Bell<Reply> bell = new Bell<Reply>();
    new Command("USER", user) {
      public void handle(Reply r) {
        if (r.code == 331)
          new Command("PASS", pass).promise(bell);
        else if (r.isComplete())
          promise(bell);
        else
          promise(bell);
      }
    };

    return bell;
  }

  // Try to authenticate with a GSS credential.
  public Bell<Reply> authenticate(GSSCredential cred) throws Exception {
    final GSSSecurityContext sec = new GSSSecurityContext(cred);
    final Bell<Reply> bell = new Bell<Reply>() {
      protected void done(Reply r) {
        if (r.isComplete())
          data.security = sec;
      }
    };

    new Command("AUTH GSSAPI") {
      public void handle(Reply r) {
        switch (r.code/100) {
          case 3:  handshake(sec, Unpooled.EMPTY_BUFFER).promise(bell);
          case 1:  return;
          case 2:  promise(bell); return;
          default: throw r.asError();
        }
      }
    };

    return bell;
  }

  // Handshake procedure for all authentication types. The input byte buffer
  // should be the raw binary token, not a Base64 encoding.
  private Bell<Reply> handshake(final SecurityContext sec, ByteBuf it) {
    final Bell<Reply> bell = new Bell<Reply>();

    try {
      ByteBuf ot = Base64.encode(sec.handshake(it), false);

      new Command("ADAT", ot.toString(data.encoding)) {
        public void handle(Reply r) throws Exception {
          switch (r.code/100) {
            case 3:
              ByteBuf token = Base64.decode(r.lines[0].skipBytes(5));
              handshake(sec, token).promise(bell);
            case 1:
              return;
            case 2:
              promise(bell); return;
            default:
              throw r.asError();
          }
        }
      };
    } catch (Exception e) {
      bell.ring(e);
    }

    return bell;
  }

  // A structure used to hold a set of features supported by a server. A fine
  // specimen of overengineering.
  // TODO: This should probably propagate control channel errors.
  class FeatureSet {
    private Set<String> features;
    private Map<String,Set<CheckBell>> checks;

    // Custom bell which is associated with a given command query.
    class CheckBell extends Bell<Boolean> {
      public final String cmd;
      CheckBell(String cmd) {
        this.cmd = cmd;
        addCheck(this);
      } public String toString() { return cmd; }
    }

    // Asynchronously check if a command is supported.
    private synchronized void init() {
      if (features != null)
        return;

      features = new HashSet<String>();
      checks = new HashMap<String,Set<CheckBell>>();

      // ...and pipe all the check commands.
      new Command("HELP") {
        public void handle(Reply r) {
          if (!r.isComplete()) return;
          for (int i = 1; i < r.length()-1; i++)
          for (String c : r.line(i).trim().toUpperCase().split(" "))
            if (!c.contains("*")) addFeature(c);
        }
      };
      new Command("HELP SITE") {
        public void handle(Reply r) {
          if (!r.isComplete()) return;
          for (int i = 1; i < r.length()-1; i++)
            addFeature(r.line(i).trim().split(" ", 2)[0].toUpperCase());
        }
      };
      //new Command("FEAT");  // TODO
      new Command(null) {
        public void handle() { finalizeChecks(); }
      };
    }

    public Bell<Boolean> supports(String cmd) {
      return new CheckBell(cmd);
    }

    // Synchronously check for command support. Used internally.
    private synchronized boolean isSupported(String cmd) {
      return features != null && features.contains(cmd);
    }

    private synchronized void addFeature(String cmd) {
      if (features == null)
        init();
      features.add(cmd);
      if (checks.containsKey(cmd)) for (CheckBell cb : checks.get(cmd))
        cb.ring(true);
    } private synchronized void addCheck(final CheckBell cb) {
      if (features == null)
        init();
      if (checks == null)
        cb.ring(isSupported(cb.cmd));
      else if (features.contains(cb.cmd))
        cb.ring(true);
      else if (!checks.containsKey(cb.cmd))
        checks.put(cb.cmd, new HashSet<CheckBell>() {{ add(cb); }});
      else
        checks.get(cb.cmd).add(cb);
    }

    // Ring this when no more commands will be issued to check support. It will
    // resolve all unsupported commands with false and update the state to
    // reflect the command list has been finalized.
    private synchronized void finalizeChecks() {
      for (Set<CheckBell> cbs : checks.values()) for (CheckBell b : cbs)
        b.ring(false);
      checks = null;
    }
  }

  // Checks if a command is supported by the server.
  public Bell<Boolean> supports(String cmd) {
    return data.features.supports(cmd);
  } private List<Bell<Boolean>> supportsMulti(String... cmd) {
    List<Bell<Boolean>> bs = new ArrayList<Bell<Boolean>>(cmd.length);
    for (String c : cmd)
      bs.add(supports(c));
    return bs;
  } public Bell<Boolean> supportsAny(String... cmd) {
    return new Bell.Or(supportsMulti(cmd));
  } public Bell<Boolean> supportsAll(String... cmd) {
    return new Bell.And(supportsMulti(cmd));
  }

  // Append the given command to the handler queue. If the command is a sync
  // command and there is nothing else in the queue, just fulfill it
  // immediately and don't put it in the queue. This will return whether the
  // command handler was appended or not (currently, it is always true).
  private boolean appendHandler(Command c) {
    synchronized (data.handlers) {
      if (!c.isSync() || !data.handlers.isEmpty())
        return data.handlers.add(c);
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
    synchronized (data.handlers) {
      assert !data.handlers.isEmpty();

      if (reply.isPreliminary()) {
        handler = data.handlers.peek();
      } else {
        handler = data.handlers.pop();

        // Remove all the syncs.
        Command peek = data.handlers.peek();
        if (peek != null && peek.isSync()) {
          syncs = new LinkedList<Command>();
          do {
            syncs.add(peek);
            data.handlers.pop();
            peek = data.handlers.peek();
          } while (peek != null && peek.isSync());
        }
      }
    }

    // Now we can call the handlers.
    handler.internalHandle(reply);
    if (syncs != null) for (Command sync : syncs)
      sync.internalHandle(null);
  }

  // Negotiate a type or change. Admittedly, this is a little unnecessarily
  // complex, and designed to defeat an edge case that will probably never be
  // encountered. Nevertheless, doing it like this ensures data channel
  // creation can be pipelined and properly handle failures to change the data
  // channel mode or type without needing to lock the channel until we're sure
  // of the mode or type change. Returns a bell that will ring with the channel
  // mode or type when the command completes.
  public synchronized Bell<Character> type(char t) {
    Bell<Character> ntb = new Bell<Character>();  // Ring with result.
    Bell<Character> otb = typeModeHelper("TYPE", t, ntb);
    data.type.promise(otb);  // Ring helper bell.
    return data.type = ntb;  // Replace current type bell.
  } public synchronized Bell<Character> mode(char m) {
    Bell<Character> nmb = new Bell<Character>();  // Ring with result.
    Bell<Character> omb = typeModeHelper("MODE", m, nmb);
    data.mode.promise(omb);  // Ring helper bell.
    return data.mode = nmb;  // Replace current mode bell.
  } private Bell<Character> typeModeHelper(
      final String cmd, final char c, final Bell<Character> cb) {
    // This bell should be rung when the current mode or type is known. Ringing
    // it will send the command which will later ring the passed bell.
    return new Bell<Character>() {
      protected void done(final char old) {
        new Command(cmd, c).promise(new Bell<Reply>() {
          protected void done(Reply r) {
            if (r.isComplete())
              cb.ring(c);
            else
              cb.ring(old);
          } protected void fail(Throwable e) {
            cb.ring(old);
          }
        });
      }
    };
  }

  // Negotiate a passive mode data channel.
  public synchronized Bell<FTPHostPort> passive() {
    final Bell<FTPHostPort> bell = new Bell<FTPHostPort>();

    new Command("PASV") {
      public void handle(Reply r) {
        if (!r.isComplete()) {
          bell.ring(r.asError());
        } try {
          String s = r.message().split("[()]")[1];
          bell.ring(new FTPHostPort(s));
        } catch (Exception e) {
          bell.ring(e);
        }
      }
    };
    
    return bell;
  }

  // This method requests a lock on the channel and returns a special view of
  // the channel once the lock request is satisfied. The special channel view
  // will have exclusive use of the channel until unlocked (after which the
  // channel lock will become unusable) or garbage collected. The channel
  // should be unlocked as soon as the command sequence requiring synchronicity
  // has been issued. Commands written to any other channels during this time
  // will have their commands deferred and sent after the channel has been
  // unlocked.
  public class Lock extends FTPChannel {
    public Lock() {
      super(FTPChannel.this);
      FTPChannel.this.new Command(null) {
        public void handle() { Lock.this.assumeControl(); }
      };
    } public Bell<FTPChannel> lock() {
      throw new IllegalStateException("cannot lock from a view");
    } public void unlock() {
      new Command(null) {
        public void handle() { FTPChannel.this.assumeControl(); }
      };
    } protected void finalize() {
      if (data.owner == this)
        FTPChannel.this.assumeControl();
    }
  }

  // This is called whenever it's the the channel's turn to become the channel
  // owner.
  protected synchronized void assumeControl() {
    synchronized (data) {
      Log.finer(data.owner.hashCode()+" -> "+hashCode());
      data.owner = this;
      if (!deferred.isEmpty()) {
        Deque<Deferred> realDeferred = deferred;
        deferred = new LinkedList<Deferred>();

        for (Deferred d : realDeferred) d.send();

        realDeferred.clear();
        realDeferred.addAll(deferred);
        deferred = realDeferred;
      }
    }
  }

  // Release the lock, if this channel is a locked view. If it's not (and the
  // base channel never is), this throws an exception.
  public void unlock() {
    throw new IllegalStateException("channel is not locked");
  }

  // A deferred command which hold onto its arguments until it is ready to be
  // written. Once it's ready to be written, call send(). If the view is not
  // the owner when send() is called, the command will once again be deferred.
  private class Deferred {
    final Command cmd;
    final Object verb;
    final Object[] args;

    Deferred(Command c, Object v, Object[] a) {
      cmd = c; verb = v; args = a;
    } void send() {
      // If we're not the owner, defer the command. Otherwise, send it.
      if (data.owner != FTPChannel.this) {
        deferred.add(this);
        Log.finer(FTPChannel.this.hashCode()+": Deferring "+this);
      } else {
        appendHandler(cmd);
        if (verb != null) channel().writeAndFlush(this);
        Log.finer(FTPChannel.this.hashCode()+": Sending "+this);
      }
    } public String toString() {
      if (verb == null)
        return "(sync)";
      StringBuilder sb = new StringBuilder(verb.toString());
      for (Object o : args)
        sb.append(" ").append(o);
      return sb.toString();
    }
  }

  // This is sort of the workhorse of the channel. Instantiate one of these to
  // send a command across this channel. The newly instantiated object will
  // serve as a "future" for the server's ultimate reply. For example, a simple
  // authorization routine could be written like:
  //
  //   FTPChannel ch = new FTPChannel(...);
  //   Reply r = ch.new Command("USER bob") {
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
    private Bell<Reply> bell = new Bell<Reply>();
    private final boolean isSync;

    // Constructing this will automatically cause the given command to be
    // written to the server. Typically, the passed cmd will be a string, but
    // can be anything. The passed arguments will be stringified and
    // concatenated with spaces for convenience. If cmd is null, nothing is
    // actually written to the server and the command serves as a sort of
    // "sync" for client code. Calling sync() on it will block until it has
    // been reached in the pipeline.
    public Command(Object verb, Object... args) {
      synchronized (data) {
        isSync = (verb == null);
        new Deferred(this, verb, args).send();
      }
    } 

    public synchronized final boolean isSync() {
      return isSync;
    } public synchronized final boolean isDone() {
      return bell.isDone();
    }

    // Promise to ring the given bell when this command is fulfilled.
    public synchronized Bell<Reply> promise(Bell<Reply> bell) {
      this.bell.promise(bell);
      return bell;
    }

    // This is called internally by the channel handler whenever a reply comes
    // in. It handles passing the received reply to handle(). Once this is
    // passed a non-preliminary reply, it sets the reply future
    private void internalHandle(Reply r) {
      try {
        synchronized (this) { handle(r); }
      } catch (Exception e) {
        bell.ring(e);
      } finally {
        if (isSync())
          bell.ring();
        else if (!r.isPreliminary())
          bell.ring(r);
      }
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

    // This may be called if the command has been deferred and cancelled
    // without ever being written, of if the caller stops caring about the
    // command.
    public void fail(Throwable t) {
      bell.ring(t);
    }

    // This is used to wait until the future has been resolved. The ultimate
    // reply will be returned, unless this was a sync command, in which case
    // null will be returned.
    public synchronized Reply sync() {
      return bell.sync();
    }
  }

  // Base class for a data channel connection to a remote server. This object
  // is itself a control channel lock, so commands can be pipelined in an
  // initializer. It is imperative to call unlock after initialization, even if
  // no commands are pipelined.
  public class DataChannel extends Lock implements Tap, Sink {
    private ChannelFuture future;
    private Throwable error;
    private Bell<FTPHostPort> hpb;

    //private char mode = data.mode.sync();
    //private char type = data.type.sync();

    private volatile Sink sink;
    private volatile boolean corked = false;

    // Negotiate a new data channel connection.
    public DataChannel() {
      passive().promise(initBell);
    } public DataChannel(Bell<FTPHostPort> hp) {
      hp.promise(initBell);
    } public DataChannel(FTPHostPort hp) {
      initBell.ring(hp);
    }

    // A handler which passes data to the sink.
    ChannelHandler reader = new SimpleChannelInboundHandler<ByteBuf>() {
      public void messageReceived(ChannelHandlerContext ctx, ByteBuf m) {
        if (sink != null) sink.drain(new Slice(m));
      } public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
        //if (sink != null) sink.drain(new ResourceException(null, t));
      } public void channelInactive(ChannelHandlerContext ctx) {
        if (sink != null) sink.drain(new Slice());
      } public void read(ChannelHandlerContext ctx) {
        if (!corked && sink != null) ctx.read();
      }
    };

    // A handler which writes slices passed to the sink.
    ChannelHandler writer = new MessageToMessageEncoder<Slice>() {
      public void encode(ChannelHandlerContext ctx, Slice msg, List out) {
        out.add(msg.plain().raw());
      }
    };

    // This bell should be rung when the channel is ready to be initialized,
    // then unset when it is.
    private Bell<FTPHostPort> initBell = new Bell<FTPHostPort>() {
      protected void done(FTPHostPort hp) {
        // Attempt to establish a channel connection to the given host/port.
        Bootstrap boot = new Bootstrap();
        boot.group(group).channel(NioSocketChannel.class);
        boot.handler(new ChannelInitializer<SocketChannel>() {
          public void initChannel(SocketChannel ch) throws Exception {
            ch.config().setConnectTimeoutMillis(timeout);
            ch.pipeline().addLast("reader", reader);
            ch.pipeline().addLast("writer", writer);
          }
        });

        future = boot.connect(hp.getAddr());
      } protected void fail(Throwable t) {
        synchronized (DataChannel.this) {
          error = t;
        }
      } protected void always() {
        initBell = null;
      }
    };

    /**
     * Get a bell that will ring when the channel is ready.
     */
    public Bell<? super DataChannel> bell() {
      return new Bell<DataChannel>().ring(this);
    }

    // Attach a sink to the tap.
    public synchronized void attach(Sink s) {
      sink = s;
    }

    // Cork or uncork the tap.
    public synchronized void cork() {
      corked = true;
    } public synchronized void uncork() {
      corked = false;
    }

    // Write a passed slice to the channel.
    public synchronized void drain(Slice s) {
      if (s.isEmpty())
        channel().close();
      else
        channel().writeAndFlush(s);
      Log.finer("Writing: "+s);
    }

    // Used internally to extract the channel from the future.
    private synchronized Channel channel() {
      if (error != null)
        throw new RuntimeException(error);
      return future.syncUninterruptibly().channel();
    }
  }

  public static void main(String[] args) throws Exception {
    FTPChannel ch = new FTPChannel(args[1]);
    java.io.BufferedReader r = new java.io.BufferedReader(
      new java.io.InputStreamReader(System.in));
    while (true)
      ch.new Command(r.readLine());
  }
}
