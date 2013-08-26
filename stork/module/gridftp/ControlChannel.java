package stork.module.gridftp;

import stork.ad.*;
import stork.util.*;
import static stork.module.ModuleException.*;
import static stork.util.StorkUtil.Static.*;
import stork.stat.*;
import stork.cred.*;

import java.util.*;
import java.net.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;
import org.globus.ftp.dc.*;

// Wrapper for the JGlobus control channel classes which abstracts away
// differences between local and remote transfers.
// TODO: Refactor.

public class ControlChannel extends Pipeline<String, Reply> {
  private FTPURI my_uri = null;
  public int port = -1;
  public final boolean local, gridftp;
  public final HackedFTPServerFacade facade;
  public final HackedControlChannel fc;
  public final BasicClientControlChannel cc;
  private Set<String> features = null;
  private boolean cmd_done = false;

  // Quick hack to handle getting peer IP from control channel.
  static class HackedControlChannel extends GridFTPControlChannel {
    public HackedControlChannel(String host, int port) {
      super(host, port);
    }

    InetAddress getIP() {
      SocketAddress sa = socket.getRemoteSocketAddress();
      return ((InetSocketAddress)sa).getAddress();
    }
  }

  // Quick hack to handle JGlobus race condition during recursive listing.
  static class HackedFTPServerFacade extends GridFTPServerFacade {
    public HackedFTPServerFacade(GridFTPControlChannel cc) {
      super(cc);
    } public TransferThreadManager createTransferThreadManager() {
      return new HackedThreadManager(
        socketPool, this, localControlChannel, gSession);
    }
  } static class HackedThreadManager extends TransferThreadManager {
    public HackedThreadManager(SocketPool sp, GridFTPServerFacade f,
      BasicServerControlChannel cc, org.globus.ftp.GridFTPSession s) {
      super(sp, f, cc, s);
      dataChannelFactory = new HackedChannelFactory();
    }
  } static class HackedChannelFactory extends GridFTPDataChannelFactory {
    public DataChannel getDataChannel(Session s, SocketBox b) {
      return new HackedDataChannel(s, b);
    }
  } static class HackedDataChannel extends GridFTPDataChannel {
    public HackedDataChannel(Session session, SocketBox socketBox) {
      super(session, socketBox);
    } public void startTransfer(DataSink sink, BasicServerControlChannel b,
      TransferContext c) throws Exception {
      transferThread = new TransferSinkThread(this, socketBox, sink, b, c) {
        public void run() {
          try {
            super.run();
          } catch (Exception e) {
            // Who cares.
          } finally {
            try { sink.close(); }
            catch (Exception e) { }
          }
        }
      }; transferThread.run();
    }
  }

  public ControlChannel(FTPURI u) {
    my_uri = u;
    port = u.port;

    if (u.file)
      throw abort("making remote connection to invalid URL");
    local = false;
    facade = null;
    gridftp = u.gridftp;

    try {
      if (u.gridftp) {
        GridFTPControlChannel gc;
        cc = fc = new HackedControlChannel(u.host, u.port);
        fc.open();

        if (u.cred != null) try {
          fc.authenticate(u.cred.credential(), null);
        } catch (Exception e) {
          throw abort("could not authenticate (certificate issue?) "+e);
        } else {
          String user = (u.user == null) ? "anonymous" : u.user;
          String pass = (u.pass == null) ? "" : u.pass;
          Reply r = exchange("USER "+user);
          if (Reply.isPositiveIntermediate(r)) try {
            execute(("PASS "+pass).trim());
          } catch (Exception e) {
            throw abort("bad password");
          } else if (!Reply.isPositiveCompletion(r)) {
            throw abort("bad username");
          }
        }

        exchange("SITE CLIENTINFO appname="+GridFTPModule.name+";appver="+
                 GridFTPModule.version+";schema=gsiftp;");
      } else {
        String user = (u.user == null) ? "anonymous" : u.user;
        String pass = (u.pass == null) ? "" : u.pass;
        cc = fc = new HackedControlChannel(u.host, u.port);
        fc.open();

        Reply r = exchange("USER "+user);
        if (Reply.isPositiveIntermediate(r)) try {
          execute("PASS "+pass);
        } catch (Exception e) {
          throw abort("bad password");
        } else if (!Reply.isPositiveCompletion(r)) {
          throw abort("bad username");
        }
      }
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw abort("couldn't establish channel: "+e.getMessage(), e);
    }
  }

  // Make a local control channel connection to a remote control channel.
  public ControlChannel(ControlChannel rc) {
    if (rc.local)
      throw abort("making local facade for local channel");
    local = true;
    gridftp = rc.gridftp;
    facade = new HackedFTPServerFacade(rc.fc);

    if (!rc.gridftp)
      facade.setDataChannelAuthentication(DataChannelAuthentication.NONE);

    cc = facade.getControlChannel();
    fc = null;
  }

  // Create a duplicate of this channel.
  public ControlChannel duplicate() {
    return new ControlChannel(my_uri);
  }

  // Dumb thing to convert mode/type chars into JGlobus mode ints...
  private static int modeIntValue(char m) {
    switch (m) {
      case 'E': return org.globus.ftp.GridFTPSession.MODE_EBLOCK;
      case 'B': return org.globus.ftp.GridFTPSession.MODE_BLOCK;
      case 'S': return org.globus.ftp.GridFTPSession.MODE_STREAM;
      default : throw abort("bad mode: "+m);
    }
  } private static int typeIntValue(char t) {
    switch (t) {
      case 'A': return Session.TYPE_ASCII;
      case 'I': return Session.TYPE_IMAGE;
      default : throw abort("bad type: "+t);
    }
  }

  // Checks if a command is supported by the channel.
  public boolean supports(String... query) {
    if (local) return false;
    
    // If we haven't cached the features, do so.
    if (features == null) {
      String r = execute("FEAT").getMessage();
      features = new HashSet<String>();
      boolean first = true;

      // Read lines from responses
      for (String s : r.split("[\r\n]+")) if (!first) {
        s = s.trim().split(" ")[0];
        if (s.startsWith("211")) break;
        if (!s.isEmpty()) features.add(s);
      } else first = false;
    }

    // Check if all features are supported.
    for (String f : query)
      if (!features.contains(f)) return false;
    return true;
  }

  // Write a command to the control channel.
  public void handleWrite(String cmd) {
    if (local) return;
    Log.fine("Write (", port, "): ", cmd);
    try {
      fc.write(new Command(cmd));
    } catch (Exception e) {
      throw abort("read error: "+e.getMessage(), e);
    }
  }

  // Read from the underlying control channel.
  public Reply readChannel() {
    try {
      return cc.read();
    } catch (Exception e) {
      throw abort("read error: "+e.getMessage(), e);
    }
  }

  // Read replies from the control channel.
  public Reply handleReply() {
    while (true) {
      Reply r = readChannel();
      Log.fine("Reply: ", r);
      if (r.getCode() < 200) addReply(r);
      else return r;
    }
  }

  // A handler for reading lists over the control channel.
  class StatHandler extends Handler {
    private Ad ad;

    public StatHandler(Ad ad) {
      this.ad = ad;
    }

    public Reply handleReply() {
      Reply r = readChannel();
      ListAdSink sink = new ListAdSink(ad, false);

      if (!Reply.isPositiveCompletion(r))
        throw abort("couldn't list: "+r);
      sink.write(r.getMessage().getBytes());
      try {
        sink.close();
      } catch (Exception e) { }
      return r;
    }
  }

  // Special handler for doing file transfers.
  class XferHandler extends Handler {
    ProgressListener pl = new ProgressListener();
    GridFTPSession sess;

    // Hack until we get something better.
    public XferHandler(GridFTPSession sess) {
      this.sess = sess;
    }

    public synchronized Reply handleReply() {
      Reply r = readChannel();

      if (!Reply.isPositivePreliminary(r)) {
        throw abort("transfer failed to start: "+r);
      } while (true) switch ((r = readChannel()).getCode()) {
        case 111:  // Restart marker
          break;   // Just ignore for now...
        case 112:  // Progress marker
          if (sess != null) sess.reportProgress(pl.parseMarker(r));
          break;
        case 226:  // Transfer complete!
          if (sess != null) sess.reportProgress(new Ad("files_done", 1));
          return r;
        default:
          throw abort("unexpected reply: "+r.getCode());
      }
    }
  }

  // Execute command, but DO throw on negative reply.
  public Reply execute(String cmd) {
    Reply r = exchange(cmd);
    if (!Reply.isPositiveCompletion(r))
      throw abort("bad reply: "+r);
    return r;
  }

  // Close the control channel.
  public void close() {
    try {
      if (local)
        facade.close();
      else
        exchange("QUIT");
      flush();
      kill();
    } catch (Exception e) {
      // Who cares.
    }
  }

  // Cancel transfers and abort the control channel as soon as possible.
  public synchronized void kill() {
    try {
      if (local)
        facade.abort();
      else
        fc.exchange(new Command("ABOR"));
    } catch (Exception e) {
      // Who cares.
    } finally {
      super.kill();
    } throw abort();
  }

  // Change the mode of this channel.
  // TODO: Detect unsupported modes.
  public void mode(char m) {
    if (local)
      facade.setTransferMode(modeIntValue(m));
    else write("MODE "+m, true);
  }

  // Change the data type of this channel.
  // TODO: Detect unsupported types.
  public void type(char t) {
    if (local)
      facade.setTransferType(typeIntValue(t));
    else write("TYPE "+t, true);
  }

  // Get the IP from the remote server as a string.
  public InetAddress getIP() {
    return fc.getIP();
  }
}
