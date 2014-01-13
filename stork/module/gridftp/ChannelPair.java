package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import stork.cred.*;
import static stork.module.ModuleException.*;

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
    if (sc.local && dc.local) {
      throw abort("file-to-file not supported");
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
      throw abort("cannot create local pair for local channel");
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

    return cp;
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
    sc.pipe("OPTS RETR Parallelism="+p+","+p+","+p+";");
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
    if (sc.supports("TREV"))
      sc.pipe("TREV PERF "+f);
    if (sc.supports("OPTS"))
      sc.pipe("OPTS RETR markers="+f+";");
    // TODO: Fallback to control channel check.
  }

  // Flush both channels so they are synchronized.
  void sync() {
    sc.sync();
    dc.sync();
  }

  // Make a directory on the destination.
  void pipeMkdir(String path, boolean ignore) {
    dc.pipe("MKD "+path);
  }

  public void close() {
    try {
      sc.close(); dc.close();
    } catch (Exception e) { /* who cares */ }
  }

  public void abortPair() {
    try {
      sc.close(); dc.close();
    } catch (Exception e) { /* who cares */ }
  }

  // Prepare the channels to transfer an XferEntry.
  // TODO: Check for extended mode support and remove session param.
  void pipeXfer(XferList.Entry e, GridFTPSession sess) {
    Log.fine("Piping: ", e);
    if (e.dir) {
      pipeMkdir(e.dpath(), true);
    } else {
      ControlChannel.TransferBell hs = sc.new TransferBell(sess);
      ControlChannel.TransferBell hd = dc.new TransferBell(sess);

      String path = e.path(), dpath = e.dpath();
      long off = e.off, len = e.len;

      // Pipe RETR
      Log.fine("RETR going to: ", sc.port);
      if (sc.local) try {
        sc.facade.retrieve(new FileMap(path, off, len));
      } catch (Exception ex) {
        throw abort(false, "could not retrieve", ex);
      } else if (len > -1) {
        sc.pipe(StorkUtil.join("ERET P", off, len, path), hs);
      } else {
        if (off > 0)
          sc.pipe("REST "+off);
        sc.pipe("RETR "+path, hs);
      }

      // Pipe STOR
      Log.fine("STOR going to: ", dc.port);
      if (dc.local) try {
        dc.facade.store(new FileMap(dpath, off, len));
      } catch (Exception ex) {
        throw abort(false, "could not store", ex);
      } else if (len > -1) {
        dc.pipe(StorkUtil.join("ESTO A", off, dpath), hd);
      } else {
        if (off > 0)
          dc.pipe("REST "+off);
        dc.pipe("STOR "+dpath, hd);
      }
    }
  }
}
