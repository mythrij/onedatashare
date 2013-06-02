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

// Class for binding a pair of control channels and performing pairwise
// operations on them.

public class ChannelPair {
  private int parallelism = 1, trev = 5, pipelining = 0;
  private char mode = 'S', type = 'A';
  private boolean dc_ready = false;

  // Remote/other view of control channels.
  // rc is always remote, oc can be either remote or local.
  ControlChannel rc, oc;

  // Source/dest view of control channels.
  // Either one of these may be local (but not both).
  ControlChannel sc, dc;

  // Create a control channel pair.
  public ChannelPair(ControlChannel sc, ControlChannel dc) {
    if (sc == null || dc == null) {
      throw new FatalEx("ChannelPair called with null args");
    } if (sc.local && dc.local) {
      throw new FatalEx("file-to-file not supported");
    } else if (sc.local) {
      rc = this.dc = dc;
      oc = this.sc = sc;
    } else if (dc.local) {
      rc = this.sc = sc;
      oc = this.dc = dc;
    } else {
      rc = this.dc = dc;
      oc = this.sc = sc;
    }
  }

  // Pair a channel with a new local channel. Note: doesn't duplicate().
  public ChannelPair(ControlChannel cc) {
    if (cc.local)
      throw new FatalEx("cannot create local pair for local channel");
    rc = dc = cc;
    oc = sc = new ControlChannel(cc);
  }

  // Get a new control channel pair based on this one.
  public ChannelPair duplicate() {
    ChannelPair cp = new ChannelPair(sc.duplicate(), dc.duplicate());
    cp.setTypeAndMode(type, mode);
    cp.setParallelism(parallelism);
    cp.setPipelining(pipelining);
    cp.setPerfFreq(trev);

    if (dc_ready) pipePassive();

    return cp;
  }

  // Our slightly better HostPort.
  private static class BetterHostPort extends HostPort {
    public byte[] bytes;  // Only the first four bytes of this are used.
    public int port;
    public BetterHostPort(String csv) {
      try {
        bytes = new byte[6];
        int i = 0;
        for (String s : csv.split(","))
          bytes[i++] = (byte) Short.parseShort(s);
        if (i != 6)
          throw new Exception(""+i);
        port = ((bytes[4]&0xFF)<<8) + (bytes[5]&0xFF);
        D("Port: "+port);
        D("      "+(port&0xFFFF));
      } catch (Exception e) {
        throw new FatalEx("malformed PASV reply", e);
      }
    } public int getPort() {
      return port & 0xFFFF;
    } public String getHost() {
      return (bytes[0]&0xFF)+"."+(bytes[1]&0xFF)+
         "."+(bytes[2]&0xFF)+"."+(bytes[3]&0xFF);
    } public String toFtpCmdArgument() {
      return (bytes[0]&0xFF)+","+(bytes[1]&0xFF)+
         ","+(bytes[2]&0xFF)+","+(bytes[3]&0xFF)+
         ","+((port&0xFF00)>>8)+","+(port&0xFF);
    }
  }

  // Pipe a PASV command to remote channel and set local channel active.
  // We should ignore the returned IP in case of a malicious server, and
  // simply subtitute the remote server's IP.
  // TODO: EPSV support.
  public void pipePassive() {
    String cmd = rc.fc.isIPv6() ? "EPSV" : "PASV";
    rc.write(cmd, rc.new Handler() {
      public Reply handleReply() {
        Reply r = rc.readChannel();
        String s = r.getMessage().split("[()]")[1];
        BetterHostPort hp = new BetterHostPort(s);

        // Make sure the IP is "close" enough to the control channel IP
        // that we should trust it. If not, change to channel IP.
        byte[] hpb = hp.bytes;
        byte[] ccb = rc.getIP().getAddress();

        D("Current HostPort: "+hp.getHost()+":"+hp.getPort());
        D("                  "+s);

        try {
          if (hpb[0] != ccb[0] || hpb[1] != ccb[1] || hpb[2] != ccb[2])
            hp.bytes = ccb;
        } catch (Exception e) {
          hp.bytes = ccb;
        }

        D("Making active connection to: "+hp.getHost()+":"+hp.getPort());

        if (oc.local) try {
          oc.facade.setActive(hp);
        } catch (Exception e) {
          throw new FatalEx(e.getMessage(), e);
        } else if (oc.fc.isIPv6()) {
          oc.execute("EPRT "+hp.toFtpCmdArgument());
        } else {
          oc.execute("PORT "+hp.toFtpCmdArgument());
        } dc_ready = true;
        return r;
      }
    });
  }

  // Set the mode and type for the pair.
  // TODO: We should check to make sure both succeeded.
  void setTypeAndMode(char t, char m) {
    if (t > 0 && type != t) {
      type = t; sc.type(t); dc.type(t);
    } if (m > 0 && mode != m) {
      mode = m; sc.mode(m); dc.mode(m);
    } sync();
  }

  // Set the parallelism for this pair.
  void setParallelism(int p) {
    //if (!rc.gridftp || parallelism == p) return;
    parallelism = p = (p < 1) ? 1 : p;
    sc.write("OPTS RETR Parallelism="+p+","+p+","+p+";", false);
  }

  // Set the pipelining for this pair.
  public void setPipelining(int p) {
    pipelining = p;
    sc.setPipelining(p);
    dc.setPipelining(p);
  }

  // Set event frequency for this pair.
  void setPerfFreq(int f) {
    //if (!rc.gridftp || trev == f) return;
    trev = f = (f < 1) ? 1 : f;
    sc.exchange("TREV PERF "+f);
    sc.exchange("OPTS RETR markers="+f+";");
  }

  // Flush both channels so they are synchronized.
  void sync() {
    sc.flush(true); dc.flush(true);
  }

  // Make a directory on the destination.
  void pipeMkdir(String path, boolean ignore) {
    if (dc.local)
      new File(path).mkdir();
    else
      dc.write("MKD "+path, ignore);
  }

  public void close() {
    try {
      sc.close(); dc.close();
    } catch (Exception e) { /* who cares */ }
  }

  public void abort() {
    try {
      sc.abort(); dc.abort();
    } catch (Exception e) { /* who cares */ }
  }

  // Prepare the channels to transfer an XferEntry.
  // TODO: Check for extended mode support and remove session param.
  void pipeXfer(XferList.Entry e, GridFTPSession sess) {
    System.out.println("Piping: "+e);
    if (e.dir) {
      pipeMkdir(e.dpath(), true);
    } else {
      ControlChannel.XferHandler hs = sc.new XferHandler(sess);
      ControlChannel.XferHandler hd = dc.new XferHandler(sess);

      String path = e.path(), dpath = e.dpath();
      long off = e.off, len = e.len;

      // Pipe RETR
      System.out.println("RETR going to: "+sc.port);
      if (sc.local) try {
        sc.facade.retrieve(new FileMap(path, off, len));
      } catch (Exception ex) {
        throw new TempEx("could not retrieve: "+ex.getMessage(), ex);
      } else if (len > -1) {
        sc.write(J("ERET P", off, len, path), hs);
      } else {
        if (off > 0)
          sc.write("REST "+off);
        sc.write("RETR "+path, hs);
      }

      // Pipe STOR
      System.out.println("STOR going to: "+dc.port);
      if (dc.local) try {
        dc.facade.store(new FileMap(dpath, off, len));
      } catch (Exception ex) {
        throw new TempEx("could not store: "+ex.getMessage(), ex);
      } else if (len > -1) {
        dc.write(J("ESTO A", off, dpath), hd);
      } else {
        if (off > 0)
          dc.write("REST "+off);
        dc.write("STOR "+dpath, hd);
      }
    }
  }
}
