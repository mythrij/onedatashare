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
      throw new Error("ChannelPair called with null args");
    } if (sc.local && dc.local) {
      throw E("file-to-file not supported");
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
  public ChannelPair(ControlChannel cc) throws Exception {
    if (cc.local)
      throw E("cannot create local pair for local channel");
    rc = dc = cc;
    oc = sc = new ControlChannel(cc);
  }

  // Get a new control channel pair based on this one.
  public ChannelPair duplicate() throws Exception {
    ChannelPair cp = new ChannelPair(sc.duplicate(), dc.duplicate());
    cp.setTypeAndMode(type, mode);
    cp.setParallelism(parallelism);
    cp.setPipelining(pipelining);
    cp.setPerfFreq(trev);

    if (dc_ready) pipePassive();

    return cp;
  }

  // Pipe a PASV command to remote channel and set local channel active.
  public void pipePassive() throws Exception {
    String cmd = rc.fc.isIPv6() ? "EPSV" : "PASV";
    rc.write(cmd, rc.new Handler() {
      public Reply handleReply() throws Exception {
        Reply r = rc.cc.read();
        String s = r.getMessage().split("[()]")[1];
        HostPort hp = new HostPort(s);

        if (oc.local)
          oc.facade.setActive(hp);
        else if (oc.fc.isIPv6())
          oc.execute("EPRT "+hp.toFtpCmdArgument());
        else
          oc.execute("PORT "+hp.toFtpCmdArgument());
        dc_ready = true;
        return r;
      }
    });
  }

  // Set the mode and type for the pair.
  void setTypeAndMode(char t, char m) throws Exception {
    if (t > 0 && type != t) {
      type = t; sc.type(t); dc.type(t);
    } if (m > 0 && mode != m) {
      mode = m; sc.mode(m); dc.mode(m);
    } sync();
  }

  // Set the parallelism for this pair.
  void setParallelism(int p) throws Exception {
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
  void setPerfFreq(int f) throws Exception {
    //if (!rc.gridftp || trev == f) return;
    trev = f = (f < 1) ? 1 : f;
    sc.exchange("TREV PERF "+f);
    sc.exchange("OPTS RETR markers="+f+";");
  }

  // Flush both channels so they are synchronized.
  void sync() throws Exception {
    sc.flush(true); dc.flush(true);
  }

  // Make a directory on the destination.
  void pipeMkdir(String path, boolean ignore) throws Exception {
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
  // TODO: Check for extended mode support.
  void pipeXfer(XferList.Entry e, TransferProgress p) throws Exception {
    System.out.println("Piping: "+e);
    if (e.dir) {
      pipeMkdir(e.dpath(), true);
    } else {
      ControlChannel.XferHandler hs = sc.new XferHandler(p);
      ControlChannel.XferHandler hd = dc.new XferHandler(p);

      String path = e.path(), dpath = e.dpath();
      long off = e.off, len = e.len;

      // Pipe RETR
      System.out.println("RETR going to: "+sc.port);
      if (sc.local) {
        sc.facade.retrieve(new FileMap(path, off, len));
      } else if (len > -1) {
        sc.write(J("ERET P", off, len, path), hs);
      } else {
        if (off > 0)
          sc.write("REST "+off);
        sc.write("RETR "+path, hs);
      }

      // Pipe STOR
      System.out.println("STOR going to: "+dc.port);
      if (dc.local) {
        dc.facade.store(new FileMap(dpath, off, len));
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
