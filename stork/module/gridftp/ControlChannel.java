package stork.module.gridftp;

import stork.ad.*;
import stork.util.*;
import static stork.module.ModuleException.*;
import static stork.util.StorkUtil.Static.*;
import stork.stat.*;
import stork.cred.*;
import stork.scheduler.*;

import java.util.*;
import java.net.*;
import java.io.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;
import org.globus.ftp.dc.*;
import org.globus.ftp.exception.*;

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

  // Scary hack to handle JGlobus race condition during recursive listing.
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
    public void activeConnect(HostPort hp, int connections) {
      SocketAddress sa = new InetSocketAddress(hp.getHost(), hp.getPort());
      Socket s = new Socket();
      SocketBox box = new ManagedSocketBox();
      socketPool.add(box);
      try {
        // Three seconds seems good, right?
        s.connect(sa, 3000);
        s.setSoTimeout(3000);
        s.setTcpNoDelay(true);

        // Do some gross authentication thing.
        org.globus.ftp.GridFTPSession ss = gSession;
        if (ss.dataChannelAuthentication != DataChannelAuthentication.NONE) {
          s = GridFTPServerFacade.authenticate(s, true, ss.credential,
            ss.dataChannelProtection, ss.dataChannelAuthentication);
        }

        synchronized (box) {
          box.setSocket(s);
        }
      } catch (SocketTimeoutException e) {
        String m = "data channel connection timed out";
        localControlChannel.write(new LocalReply(421, m));
        throw abort(m);
      } catch (Exception e) {
        String m = "could not establish data channel connection";
        localControlChannel.write(new LocalReply(421, m));
        throw abort(m, e);
      }
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
            try { sink.close(); }
            catch (Exception e2) { }
          }
        }
      }; transferThread.run();
    }
  }

  public ControlChannel(EndPoint e) {
    this(new FTPURI(e));
  } public ControlChannel(FTPURI u) {
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
          fc.authenticate(u.cred.credential(), u.user);
        } catch (Exception e) {
          throw abort("could not authenticate", e);
        } else {
          String user = (u.user == null) ? "anonymous" : u.user;
          String pass = (u.pass == null) ? "none" : u.pass;
          Reply r = pipe("USER "+user).waitFor();
          if (Reply.isPositiveIntermediate(r)) try {
            pipe(("PASS "+pass).trim()).waitFor();
          } catch (Exception e) {
            throw abort("bad password", e);
          } else if (!Reply.isPositiveCompletion(r)) {
            throw abort("bad username: "+r);
          }
        }

        pipe("SITE CLIENTINFO appname="+GridFTPModule.name+";appver="+
             GridFTPModule.version+";schema=gsiftp;");
      } else {
        String user = (u.user == null) ? "anonymous" : u.user;
        String pass = (u.pass == null) ? "none" : u.pass;
        cc = fc = new HackedControlChannel(u.host, u.port);
        fc.open();

        Reply r = pipe("USER "+user).waitFor();
        if (Reply.isPositiveIntermediate(r)) try {
          pipe(("PASS "+pass).trim()).waitFor();
        } catch (Exception e) {
          throw abort("bad password", e);
        } else if (!Reply.isPositiveCompletion(r)) {
          throw abort("bad username: "+r);
        }
      }
    } catch (UnknownHostException e) {
      throw abort("could not resolve host: "+u.host);
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw abort("could not establish control channel", e);
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
      Bell<Reply> hb = pipe("HELP");
      Bell<Reply> fb = pipe("FEAT");
      features = new HashSet<String>();

      // Read the HELP response.
      try {
        System.out.println(hb.waitFor().getMessage());
        String r = hb.waitFor().getMessage();
        boolean first = true;

        for (String s : r.split("[\r\n]+")) if (!first) {
          if (s.startsWith("2")) break;
          String[] cmds = s.trim().split(" ");
          features.addAll(Arrays.asList(cmds));
        } else first = false;
      } catch (Exception e) {
        // This should never happen, but if it does, oh well.
      }

      // Read the FEAT response.
      try {
        String r = fb.waitFor().getMessage();
        boolean first = true;

        for (String s : r.split("[\r\n]+")) if (!first) {
          if (s.startsWith("2")) break;
          s = s.trim().split(" ")[0];
          features.add(s);
        } else first = false;
      } catch (Exception e) {
        // This should never happen either, but...
      }

      // Just a little clean-up...
      features.remove("");
    }

    // Check if all features are supported.
    for (String f : query)
      if (!features.contains(f)) return false;
    return true;
  }

  // Write a command to the control channel.
  // TODO: Make this non-blocking.
  public boolean handleWrite(String cmd) {
    Log.fine("Write: ", cmd);
    //new Exception().printStackTrace();
    try {
      fc.write(new Command(cmd));
    } catch (Exception e) {
      kill();
      throw abort("write error", e);
    } return true;
  }

  // Read replies from the control channel.
  public Reply handleReply(String cmd) {
    try {
      Reply r = cc.read();
      Log.fine("Reply: ", r); 
      if (r.getCode() >= 500)
        throw abort(true, r.getMessage());
      if (r.getCode() >= 400)
        throw abort(false, r.getMessage());
      return r;
    } catch (IOException e) {
      throw abort("read error", e);
    } catch (ServerException e) {
      throw abort("server", e);
    } catch (FTPReplyParseException e) {
      throw abort("server", e);
    }
  }

  // Special handler for doing file transfers.
  class TransferBell extends Bell<Reply> {
    ProgressListener pl = new ProgressListener();
    GridFTPSession sess;

    // Hack until we get something better.
    public TransferBell() {
      this(null);
    } public TransferBell(GridFTPSession sess) {
      this.sess = sess;
    }

    // Filter markers from ringing the bell.
    public boolean filter(Reply r, Throwable t) {
      if (t != null || r == null) return true;
      int c = r.getCode();
      switch (c) {
        case 111:  // Restart marker
          return false;  // Just ignore for now...
        case 112:  // Progress marker
          if (sess != null) sess.reportProgress(pl.parseMarker(r));
      } return c >= 200;
    }

    public void done(Reply r) {
      int c = r.getCode();
      if (c >= 200 && c < 300) {
        if (sess != null)
          sess.reportProgress(new Ad("files_done", 1));
      } else {
        throw abort("unexpected reply: "+c);
      }
    }
  }

  // Close the control channel.
  public Bell<Reply> close() {
    Bell<Reply> rb;
    if (local) try {
      facade.close();
    } catch (Exception e) {
      // Just ignore it.
    } finally {
      rb = new Bell<Reply>(null);
    } else {
      rb = pipe("QUIT");
    } return rb;
  }

  // Cancel transfers and abort the control channel as soon as possible.
  public synchronized void abortTransfers() {
    try {
      if (local)
        facade.abort();
      else
        fc.exchange(new Command("ABOR"));
    } catch (Exception e) {
      // Who cares.
    } finally {
      kill();
    } throw abort();
  }

  // Change the mode of this channel.
  // TODO: Detect unsupported modes.
  public Bell<Reply> mode(char m) {
    if (local) {
      facade.setTransferMode(modeIntValue(m));
      return new Bell<Reply>(null);
    } else {
      return pipe("MODE "+m);
    }
  }

  // Change the data type of this channel.
  // TODO: Detect unsupported types.
  public Bell<Reply> type(char t) {
    if (local) {
      facade.setTransferType(typeIntValue(t));
      return new Bell<Reply>(null);
    } else {
      return pipe("TYPE "+t);
    }
  }

  // Get the IP from the remote server as a string.
  public InetAddress getIP() {
    return fc.getIP();
  }
}
