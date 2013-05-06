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
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

// Wrapper for the JGlobus control channel classes which abstracts away
// differences between local and remote transfers.
// TODO: Refactor.

public class ControlChannel extends Pipeline<String, Reply> {
  private FTPURI my_uri = null;
  public int port = -1;
  public final boolean local, gridftp;
  public final FTPServerFacade facade;
  public final FTPControlChannel fc;
  public final BasicClientControlChannel cc;
  private Set<String> features = null;
  private boolean cmd_done = false;

  public ControlChannel(FTPURI u) throws Exception {
    my_uri = u;
    port = u.port;

    if (u.file)
      throw new Error("making remote connection to invalid URL");
    local = false;
    facade = null;
    gridftp = u.gridftp;

    if (u.gridftp) {
      GridFTPControlChannel gc;
      cc = fc = gc = new GridFTPControlChannel(u.host, u.port);
      gc.open();

      if (u.cred != null) try {
        gc.authenticate(u.cred.credential(), u.user);
      } catch (Exception e) {
        throw E("could not authenticate (certificate issue?) "+e);
      } else {
        String user = (u.user == null) ? "anonymous" : u.user;
        String pass = (u.pass == null) ? "" : u.pass;
        Reply r = exchange("USER "+user);
        if (Reply.isPositiveIntermediate(r)) try {
          execute(("PASS "+pass).trim());
        } catch (Exception e) {
          throw E("bad password");
        } else if (!Reply.isPositiveCompletion(r)) {
          throw E("bad username");
        }
      }

      exchange("SITE CLIENTINFO appname="+GridFTPModule.info_ad.name+
               ";appver="+GridFTPModule.info_ad.version+";schema=gsiftp;");
    } else {
      String user = (u.user == null) ? "anonymous" : u.user;
      String pass = (u.pass == null) ? "" : u.pass;
      cc = fc = new FTPControlChannel(u.host, u.port);
      fc.open();

      Reply r = exchange("USER "+user);
      if (Reply.isPositiveIntermediate(r)) try {
        execute("PASS "+pass);
      } catch (Exception e) {
        throw E("bad password");
      } else if (!Reply.isPositiveCompletion(r)) {
        throw E("bad username");
      }
    }
  }

  // Make a local control channel connection to a remote control channel.
  public ControlChannel(ControlChannel rc) throws Exception {
    if (rc.local)
      throw new Error("making local facade for local channel");
    local = true;
    gridftp = rc.gridftp;
    if (gridftp)
      facade = new GridFTPServerFacade((GridFTPControlChannel) rc.fc);
    else
      facade = new FTPServerFacade(rc.fc);
    cc = facade.getControlChannel();
    fc = null;
  }

  // Create a duplicate of this channel.
  public ControlChannel duplicate() throws Exception {
    return new ControlChannel(my_uri);
  }

  // Dumb thing to convert mode/type chars into JGlobus mode ints...
  private static int modeIntValue(char m) throws Exception {
    switch (m) {
      case 'E': return org.globus.ftp.GridFTPSession.MODE_EBLOCK;
      case 'B': return org.globus.ftp.GridFTPSession.MODE_BLOCK;
      case 'S': return org.globus.ftp.GridFTPSession.MODE_STREAM;
      default : throw new Error("bad mode: "+m);
    }
  } private static int typeIntValue(char t) throws Exception {
    switch (t) {
      case 'A': return Session.TYPE_ASCII;
      case 'I': return Session.TYPE_IMAGE;
      default : throw new Error("bad type: "+t);
    }
  }

  // Checks if a command is supported by the channel.
  public boolean supports(String... query) throws Exception {
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
  public void handleWrite(String cmd) throws Exception {
    if (local) return;
    System.out.println("Write ("+port+"): "+cmd);
    fc.write(new Command(cmd));
  }

  // Read replies from the control channel.
  public Reply handleReply() throws Exception {
    while (true) {
      Reply r = cc.read();
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

    public synchronized Reply handleReply() throws Exception {
      Reply r = cc.read();

      if (!Reply.isPositivePreliminary(r)) {
        throw E("transfer failed to start: "+r);
      } while (true) switch ((r = cc.read()).getCode()) {
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
          throw E("unexpected reply: "+r.getCode());
      }
    }
  }

  // Execute command, but DO throw on negative reply.
  public Reply execute(String cmd) throws Exception {
    Reply r = exchange(cmd);
    if (!Reply.isPositiveCompletion(r))
      throw E("bad reply: "+r);
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

  public void abort() throws Exception {
    if (local)
      facade.abort();
    else
      exchange("ABOR");
    kill();
  }

  // Change the mode of this channel.
  // TODO: Detect unsupported modes.
  public void mode(char m) throws Exception {
    if (local)
      facade.setTransferMode(modeIntValue(m));
    else write("MODE "+m, true);
  }

  // Change the data type of this channel.
  // TODO: Detect unsupported types.
  public void type(char t) throws Exception {
    if (local)
      facade.setTransferType(typeIntValue(t));
    else write("TYPE "+t, true);
  }
}
