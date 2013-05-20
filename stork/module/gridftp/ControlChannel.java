package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import static stork.util.StorkUtil.Static.*;
import stork.stat.*;
import stork.cred.*;

import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;
import org.globus.ftp.dc.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

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

    String getIP() {
      SocketAddress sa = socket.getRemoteSocketAddress();
      return ((InetSocketAddress)sa).getAddress().getHostAddress();
    }
  }

  // Quick hack to handle JGlobus race condition during recursive listing.
  static class HackedFTPServerFacade extends GridFTPServerFacade {
    public TransferThreadManager createTransferThreadManager() {
      return new HackedThreadManager(
        socketPool, this, localControlChannel, gSession);
    } public HackedFTPServerFacade(GridFTPControlChannel cc) {
      super(cc);
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
      throw new FatalEx("making remote connection to invalid URL");
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
          throw new FatalEx("could not authenticate (certificate issue?) "+e);
        } else {
          String user = (u.user == null) ? "anonymous" : u.user;
          String pass = (u.pass == null) ? "" : u.pass;
          Reply r = exchange("USER "+user);
          if (Reply.isPositiveIntermediate(r)) try {
            execute(("PASS "+pass).trim());
          } catch (Exception e) {
            throw new FatalEx("bad password");
          } else if (!Reply.isPositiveCompletion(r)) {
            throw new FatalEx("bad username");
          }
        }

        exchange("SITE CLIENTINFO appname="+GridFTPModule.info_ad.name+
                 ";appver="+GridFTPModule.info_ad.version+";schema=gsiftp;");
      } else {
        String user = (u.user == null) ? "anonymous" : u.user;
        String pass = (u.pass == null) ? "" : u.pass;
        cc = fc = new HackedControlChannel(u.host, u.port);
        fc.open();

        Reply r = exchange("USER "+user);
        if (Reply.isPositiveIntermediate(r)) try {
          execute("PASS "+pass);
        } catch (Exception e) {
          throw new FatalEx("bad password");
        } else if (!Reply.isPositiveCompletion(r)) {
          throw new FatalEx("bad username");
        }
      }
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new FatalEx("couldn't establish channel: "+e.getMessage(), e);
    }
  }

  // Make a local control channel connection to a remote control channel.
  public ControlChannel(ControlChannel rc) {
    if (rc.local)
      throw new FatalEx("making local facade for local channel");
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
      default : throw new FatalEx("bad mode: "+m);
    }
  } private static int typeIntValue(char t) {
    switch (t) {
      case 'A': return Session.TYPE_ASCII;
      case 'I': return Session.TYPE_IMAGE;
      default : throw new FatalEx("bad type: "+t);
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
    System.out.println("Write ("+port+"): "+cmd);
    try {
      fc.write(new Command(cmd));
    } catch (Exception e) {
      throw new FatalEx("read error: "+e.getMessage(), e);
    }
  }

  // Read from the underlying control channel.
  public Reply readChannel() {
    try {
      return cc.read();
    } catch (Exception e) {
      throw new FatalEx("read error: "+e.getMessage(), e);
    }
  }

  // Read replies from the control channel.
  public Reply handleReply() {
    while (true) {
      Reply r = readChannel();
      System.out.println("Reply: "+r);
      if (r.getCode() < 200) addReply(r);
      else return r;
    }
  }

  // Special handler for doing file transfers.
  class XferHandler extends Handler {
    ProgressListener pl = null;

    public XferHandler(TransferProgress p) {
      if (p != null) pl = new ProgressListener(p);
    }

    public synchronized Reply handleReply() {
      Reply r = readChannel();

      if (!Reply.isPositivePreliminary(r)) {
        throw new FatalEx("transfer failed to start: "+r);
      } while (true) switch ((r = readChannel()).getCode()) {
        case 111:  // Restart marker
          break;   // Just ignore for now...
        case 112:  // Progress marker
          if (pl != null)
            pl.markerArrived(new PerfMarker(r.getMessage()));
          D("Got marker:", r);
          if (pl == null)
            D("But no progress listener...");
          break;
        case 226:  // Transfer complete!
          return r;
        default:
          throw new FatalEx("unexpected reply: "+r.getCode());
      }
    }
  }

  // Execute command, but DO throw on negative reply.
  public Reply execute(String cmd) {
    Reply r = exchange(cmd);
    if (!Reply.isPositiveCompletion(r))
      throw new FatalEx("bad reply: "+r);
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

  public void abort() {
    try {
      if (local)
        facade.abort();
      else
        exchange("ABOR");
      kill();
    } catch (Exception e) {
      // Who cares.
    }
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
  public String getIP() {
    return fc.getIP();
  }
}
