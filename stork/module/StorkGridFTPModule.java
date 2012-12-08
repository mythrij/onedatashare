package stork.module;

import stork.util.*;
import stork.stat.*;
import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

public class StorkGridFTPModule extends TransferModule {
  private static ModuleInfoAd info_ad;

  static {
    ClassAd ad = new ClassAd();
    ad.insert("name", "Stork GridFTP Module");
    ad.insert("version", "0.1");
    ad.insert("author", "Brandon Ross");
    ad.insert("email", "bwross@buffalo.edu");
    ad.insert("description",
              "A rudimentary module for FTP and GridFTP transfers.");
    ad.insert("protocols", "gsiftp,gridftp,ftp");
    ad.insert("accepts", "classads");
    ad.insert("opt_params", "parallelism,x509_proxy,optimizer");

    try {
      info_ad = new ModuleInfoAd(ad);
    } catch (Exception e) {
      info_ad = null;
      System.out.println("Fatal error parsing StorkGridFTPModule info ad");
      System.exit(1);
    }
  }

  // A combined sink/source for file I/O.
  static class FileMap implements DataSink, DataSource {
    RandomAccessFile file;
    long rem, total, base;
    
    public FileMap(String path, long off, long len) throws IOException {
      file = new RandomAccessFile(path, "rw");
      base = off;
      if (off > 0) file.seek(off);
      if (len+off >= file.length()) len = -1;
      total = rem = len;
    } public FileMap(String path, long off) throws IOException {
      this(path, off, -1);
    } public FileMap(String path) throws IOException {
      this(path, 0, -1);
    }

    public void write(Buffer buffer) throws IOException {
      if (buffer.getOffset() >= 0)
        file.seek(buffer.getOffset());
      file.write(buffer.getBuffer());
    }

    public Buffer read() throws IOException {
      if (rem == 0) return null;
      int len = (rem > 0x3FFF || rem < 0) ? 0x3FFF : (int) rem;
      byte[] b = new byte[len];
      long off = file.getFilePointer() - base;
      len = file.read(b);
      if (len < 0) return null;
      if (rem > 0) rem -= len;
      return new Buffer(b, len, off);
    }
      
    public void close() throws IOException {
      file.close();
    }

    public long totalSize() throws IOException {
      return (total < 0) ? file.length() : total;
    }
  }

  // A sink meant to receive MLSD lists. It contains a list of
  // JGlobus Buffers (byte buffers with offsets) that it reads
  // through sequentially using a BufferedReader to read lines
  // and parse data returned by FTP and GridFTP MLSD commands.
  static class ListSink extends Reader implements DataSink {
    private String base;
    private LinkedList<Buffer> buf_list;
    private Buffer cur_buf = null;
    private BufferedReader br;
    private int off = 0;

    public ListSink(String base) {
      this.base = base;
      buf_list = new LinkedList<Buffer>();
      br = new BufferedReader(this);
    }

    public void write(Buffer buffer) throws IOException {
      buf_list.add(buffer);
    }

    public void close() throws IOException { }

    private Buffer nextBuf() {
      try {
        return cur_buf = buf_list.pop();
      } catch (Exception e) {
        return cur_buf = null;
      }
    }

    // Increment reader offset, getting new buffer if needed.
    private void skip(int amt) {
      off += amt;

      // See if we need a new buffer from the list.
      while (cur_buf != null && off >= cur_buf.getLength()) {
        off -= cur_buf.getLength();
        nextBuf();
      }
    }

    // Read some bytes from the reader into a char array.
    public int read(char[] cbuf, int co, int cl) throws IOException {
      if (cur_buf == null && nextBuf() == null)
        return -1;

      byte[] bbuf = cur_buf.getBuffer();
      int bl = bbuf.length - off;
      int len = (bl < cl) ? bl : cl;

      for (int i = 0; i < len; i++)
        cbuf[co+i] = (char) bbuf[off+i];

      skip(len);

      // If we can write more, write more.
      if (len < cl && cur_buf != null)
        len += read(cbuf, co+len, cl-len);

      return len;
    }

    // Read a line, updating offset.
    private String readLine() {
      try  {
        String s = br.readLine();
        return s;
      } catch (Exception e) { return null; }
    }

    // Get the list from the sink as an XferList.
    public XferList getList(String dir) {
      XferList xl = new XferList(base, "");
      String line;

      // Read lines from the buffer list.
      while ((line = readLine()) != null) {
        try {
          MlsxEntry m = new MlsxEntry(line);

          String name = m.getFileName();
          String type = m.get("type");
          String size = m.get("size");

          if (type.equals(m.TYPE_FILE))
            xl.add(dir+name, Long.parseLong(size));
          else if (!name.equals(".") && !name.equals(".."))
            xl.add(dir+name);
        } catch (Exception e) {
          e.printStackTrace();
          continue;  // Weird data I guess!
        }
      } return xl;
    }
  }

  // Wrapper for the JGlobus control channel classes which abstracts away
  // differences between local and remote transfers.
  private static class ControlChannel {
    public final boolean local, gridftp;
    public final FTPServerFacade facade;
    public final FTPControlChannel fc;
    public final BasicClientControlChannel cc;

    public ControlChannel(FTPURI u) throws Exception {
      if (u.file)
        throw new Error("making remote connection to invalid URL");
      local = false;
      facade = null;
      gridftp = u.gridftp;

      if (u.gridftp) {
        GridFTPControlChannel gc;
        cc = fc = gc = new GridFTPControlChannel(u.host, u.port);
        gc.open();

        if (u.cred != null) {
          gc.authenticate(u.cred, u.user);
        } else {
          Reply r = exchange("USER", u.user);
          if (Reply.isPositiveIntermediate(r)) try {
            execute("PASS", u.pass);
          } catch (Exception e) {
            throw new Exception("bad password");
          } else if (!Reply.isPositiveCompletion(r)) {
            throw new Exception("bad username");
          }
        }

        exchange("SITE CLIENTINFO appname="+info_ad.name+
                 ";appver="+info_ad.version+";schema=gsiftp;");
      } else {
        String user = (u.user == null) ? "anonymous" : u.user;
        cc = fc = new FTPControlChannel(u.host, u.port);
        fc.open();

        Reply r = exchange("USER", user);
        if (Reply.isPositiveIntermediate(r)) try {
          execute("PASS", u.pass);
        } catch (Exception e) {
          throw new Exception("bad password");
        } else if (!Reply.isPositiveCompletion(r)) {
          throw new Exception("bad username");
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

    // Dumb thing to convert mode/type chars into JGlobus mode ints...
    private static int modeIntValue(char m) throws Exception {
      switch (m) {
        case 'E': return GridFTPSession.MODE_EBLOCK;
        case 'B': return GridFTPSession.MODE_BLOCK;
        case 'S': return GridFTPSession.MODE_STREAM;
        default : throw new Error("bad mode: "+m);
      }
    } private static int typeIntValue(char t) throws Exception {
      switch (t) {
        case 'A': return Session.TYPE_ASCII;
        case 'I': return Session.TYPE_IMAGE;
        default : throw new Error("bad type: "+t);
      }
    }

    // Change the mode of this channel.
    public void mode(char m) throws Exception {
      if (local)
        facade.setTransferMode(modeIntValue(m));
      else execute("MODE", m);
    }

    // Change the data type of this channel.
    public void type(char t) throws Exception {
      if (local)
        facade.setTransferType(typeIntValue(t));
      else execute("TYPE", t);
    }

    // Pipe a command whose reply will be read later.
    public void write(Object... args) throws Exception {
      if (local) return;
      System.out.println("Write: "+StorkUtil.join(args));
      fc.write(new Command(StorkUtil.join(args)));
    }

    // Read the reply of a piped command.
    public Reply read() throws Exception {
      Reply r = cc.read();
      System.out.println("Read: "+r);
      return r;
    }

    // Execute command, but don't throw on negative reply.
    public Reply exchange(Object... args) throws Exception {
      if (local) return null;
      return fc.exchange(new Command(StorkUtil.join(args)));
    }

    // Execute command, but DO throw on negative reply.
    public Reply execute(Object... args) throws Exception {
      if (local) return null;
      return fc.execute(new Command(StorkUtil.join(args)));
    }

    // Close the control channels in the chain.
    public void close() throws Exception {
      if (local) {
        facade.close();
      } else {
        write("QUIT");
      }
    }

    public void abort() throws Exception {
      if (local) {
        facade.abort();
      } else {
        write("ABOR");
      }
    }
  }

  // Class for binding a pair of control channels and performing pairwise
  // operations on them.
  private static class ChannelPair {
    public final FTPURI su, du;
    public final boolean gridftp;
    private int parallelism = 1, trev = 5;
    private char mode = 'S', type = 'A';
    private boolean dc_ready = false;

    // Remote/other view of control channels.
    // rc is always remote, oc can be either remote or local.
    private ControlChannel rc, oc;

    // Source/dest view of control channels.
    // Either one of these may be local (but not both).
    private ControlChannel sc, dc;

    // Create a control channel pair. TODO: Check if they can talk.
    public ChannelPair(FTPURI su, FTPURI du) throws Exception {
      this.su = su; this.du = du;
      gridftp = !su.ftp && !du.ftp;

      if (su == null || du == null) {
        throw new Error("ChannelPair called with null args");
      } if (su.file && du.file) {
        throw new Exception("file-to-file not supported");
      } else if (su.file) {
        rc = dc = new ControlChannel(du);
        oc = sc = new ControlChannel(rc);
      } else if (du.file) {
        rc = sc = new ControlChannel(su);
        oc = dc = new ControlChannel(rc);
      } else {
        rc = dc = new ControlChannel(du);
        oc = sc = new ControlChannel(su);
      }
    }

    // Pair a channel with a new local channel. Note: don't duplicate().
    public ChannelPair(ControlChannel cc) throws Exception {
      if (cc.local)
        throw new Error("cannot create local pair for local channel");
      du = null; su = null;
      gridftp = cc.gridftp;
      rc = dc = cc;
      oc = sc = new ControlChannel(cc);
    }

    // Get a new control channel pair based on this one.
    public ChannelPair duplicate() throws Exception {
      ChannelPair cp = new ChannelPair(su, du);
      cp.setTypeAndMode(type, mode);
      cp.setParallelism(parallelism);
      cp.setPerfFreq(trev);

      if (dc_ready) {
        HostPort hp = cp.setPassive();
        cp.setActive(hp);
      }

      return cp;
    }

    public void pipePassive() throws Exception {
      rc.write(rc.fc.isIPv6() ? "EPSV" : "PASV");
    }

    // Read and handle the response of a pipelined PASV.
    public HostPort getPasvReply() throws Exception {
      Reply r = rc.read();
      String s = r.getMessage().split("[()]")[1];
      return new HostPort(s);
    }

    public HostPort setPassive() throws Exception {
      pipePassive();
      return getPasvReply();
    }

    // Put the other channel into active mode.
    void setActive(HostPort hp) throws Exception {
      if (oc.local)
        oc.facade.setActive(hp);
      else if (oc.fc.isIPv6())
        oc.execute(new Command("EPRT", hp.toFtpCmdArgument()));
      else
        oc.execute(new Command("PORT", hp.toFtpCmdArgument()));
      dc_ready = true;
    }

    // Set the mode and type for the pair.
    void setTypeAndMode(char t, char m) throws Exception {
      if (t > 0 && type != t) {
        type = t; sc.type(t); dc.type(t);
      } if (m > 0 && mode != m) {
        mode = m; sc.mode(m); dc.mode(m);
      }
    }

    // Set the parallelism for this pair.
    void setParallelism(int p) throws Exception {
      if (!rc.gridftp || parallelism == p) return;
      parallelism = p = (p < 1) ? 1 : p;
      rc.execute("OPTS RETR Parallelism="+p+","+p+","+p+";");
    }

    // Set event frequency for this pair.
    void setPerfFreq(int f) throws Exception {
      if (!rc.gridftp || trev == f) return;
      trev = f = (f < 1) ? 1 : f;
      rc.exchange("TREV", "PERF", f);
    }

    // Make a directory on the destination.
    void pipeMkdir(String path) throws Exception {
      if (dc.local)
        new File(path).mkdir();
      else
        dc.write("MKD", path);
    }

    // Prepare the channels to transfer an XferEntry.
    void pipeTransfer(XferList.Entry e) throws Exception {
      if (e.dir) {
        pipeMkdir(e.dpath());
      } else {
        pipeRetr(e.path(), e.off, e.len);
        pipeStor(e.dpath(), e.off, e.len);
      }
    }

    // Prepare the source to retrieve a file.
    // FIXME: Check for ERET/REST support.
    void pipeRetr(String path, long off, long len) throws Exception {
      if (sc.local) {
        sc.facade.retrieve(new FileMap(path, off, len));
      } else if (len > -1) {
        sc.write("ERET", "P", off, len, path);
      } else {
        if (off > 0)
          sc.write("REST", off);
        sc.write("RETR", path);
      }
    }

    // Prepare the destination to store a file.
    // FIXME: Check for ESTO/REST support.
    void pipeStor(String path, long off, long len) throws Exception {
      if (dc.local) {
        dc.facade.store(new FileMap(path, off, len));
      } else if (len > -1) {
        dc.write("ESTO", "A", off, path);
      } else {
        if (off > 0)
          dc.write("REST", off);
        dc.write("STOR", path);
      }
    }

    // Watch a transfer as it takes place, intercepting status messages
    // and reporting any errors. Use this for pipelined transfers.
    // TODO: I'm sure this can be done better...
    void watchTransfer(TransferProgress p) throws Exception {
      MonitorThread rmt, omt;

      rmt = new MonitorThread(rc);
      omt = new MonitorThread(oc);

      rmt.pair(omt);
      rmt.progress = p;

      omt.start();
      rmt.run();
      omt.join();

      if (omt.error != null)
        throw omt.error;
      if (rmt.error != null)
        throw rmt.error;
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
  }

  // A better transfer monitor than the crappy one JGlobus provides.
  // TODO: Dead thread detection.
  static class MonitorThread extends Thread {
    private ControlChannel cc;
    public TransferProgress progress = null;
    private MonitorThread other = this;
    public Exception error = null;

    public MonitorThread(ControlChannel cc) {
      this.cc = cc;
    }

    public void pair(MonitorThread other) {
      this.other = other;
      other.other = this;
    }

    public void process() throws Exception {
      Reply r = cc.read();

      if (progress == null)
        progress = new TransferProgress();

      ProgressListener pl = new ProgressListener(progress);

      if (other.error != null)
        throw other.error;

      if (!Reply.isPositivePreliminary(r)) {
        error = new Exception("failed to start");
      } while (other.error == null) switch ((r = cc.read()).getCode()) {
        case 111:  // Restart marker
          break;   // Just ignore for now...
        case 112:  // Progress marker
          pl.markerArrived(new PerfMarker(r.getMessage()));
          break;
        case 226:  // Transfer complete!
          return;
        default:
          throw new Exception("unexpected reply: "+r.getCode());
      } throw other.error;  // We'd have returned otherwise...
    }

    public void run() {
      try {
        process();
      } catch (Exception e) {
        error = e;
      }
    }
  }

  // Wraps a URI and a credential into one object and makes sure the URI
  // represents a supported protocol. Also parses out a bunch of stuff.
  private static class FTPURI {
    public final URI uri;
    public final GSSCredential cred;

    public final boolean gridftp, ftp, file;
    public final String host, proto;
    public final int port;
    public final String user, pass;
    public final String path;

    public FTPURI(URI uri, GSSCredential cred) throws Exception {
      this.uri = uri; this.cred = cred;
      host = uri.getHost();
      proto = uri.getScheme();
      int p = uri.getPort();
      String ui = uri.getUserInfo();

      if (uri.getPath().startsWith("/~"))
        path = uri.getPath().substring(1);
      else
        path = uri.getPath();

      // Check protocol and determine port.
      if (proto == null || proto.isEmpty()) {
        throw new Exception("no protocol specified");
      } if ("gridftp".equals(proto) || "gsiftp".equals(proto)) {
        port = (p > 0) ? p : 2811;
        gridftp = true; ftp = false; file = false;
      } else if ("ftp".equals(proto)) {
        port = (p > 0) ? p : 21;
        gridftp = false; ftp = true; file = false;
      } else if ("file".equals(proto)) {
        port = -1;
        gridftp = false; ftp = false; file = true;
      } else {
        throw new Exception("unsupported protocol: "+proto);
      }

      // Determine username and password.
      if (ui != null && !ui.isEmpty()) {
        int i = ui.indexOf(':');
        user = (i < 0) ? ui : ui.substring(0,i);
        pass = (i < 0) ? "" : ui.substring(i+1);
      } else {
        user = pass = null;
      }
    }
  }

  // A custom extended GridFTPClient that implements some undocumented
  // operations and provides some more responsive transfer methods.
  private static class StorkFTPClient {
    private FTPURI su, du;
    private TransferProgress progress = new TransferProgress();
    private AdSink sink = null;
    private FTPServerFacade local;
    private ChannelPair cc;  // Main control channels.
    private LinkedList<ChannelPair> ccs;

    private int parallelism = 1, pipelining = 50,
                concurrency = 1, trev = 5;
    private Range para_range = new Range(3, 64);
    private Optimizer optimizer = null;

    volatile boolean aborted = false;

    public StorkFTPClient(FTPURI su, FTPURI du) throws Exception {
      this.su = su; this.du = du;
      cc = new ChannelPair(su, du);
      ccs = new LinkedList<ChannelPair>();
      ccs.add(cc);
    }

    // Set the progress listener for this client's transfers.
    public void setAdSink(AdSink sink) {
      this.sink = sink;
      progress.attach(sink);
    }

    // Set parallelism for every channel pair.
    public void setParallelism(int p) {
      p = (p < 1) ? 1 : p;
      if (cc.dc.local) {
        setConcurrency(p);
      } else for (ChannelPair c : ccs) try {
        c.setParallelism(p);
      } catch (Exception e) {
        System.out.println("Wasn't able to set parallelism to "+p+"...");
        e.printStackTrace();
        return;
      } parallelism = p;
    }

    public void setParallelismRange(int lo, int hi) throws Exception {
      if (lo < 1 || hi < 1)
        throw new Exception("parallelism must be a positive value");
      para_range = new Range(lo, hi);
    }

    public void setPipelining(int p) {
      p = (p < 0) ? 0 : p;
      pipelining = p;
    }

    public void setConcurrency(int c) {
      c = (c < 1) ? 1 : c;
      try {
        while (ccs.size() > c)
          ccs.removeLast().close();
        while (ccs.size() < c)
          ccs.add(cc.duplicate());
      } catch (Exception e) {
        System.out.println("Wasn't able to set concurrency to "+c+"...");
        e.printStackTrace();
        return;
      } concurrency = c;
    }

    // Some crazy undocumented voodoo magick!
    void setPerfFreq(int f) {
      trev = f = (f < 1) ? 1 : f;
      for (ChannelPair c : ccs) try {
        c.setPerfFreq(f);
      } catch (Exception e) { /* oh well */ }
    }

    // Set the optimizer this client should use.
    void setOptimizer(Optimizer o) {
      optimizer = o;
    }

    // Close control channel.
    void close() {
      cc.close();
    }

    // Recursively list directories.
    public XferList mlsr(String path) throws Exception {
      final String MLSR = "MLSR", MLSD = "MLSD";
      //String cmd = isFeatureSupported("MLSR") ? MLSR : MLSD;
      String cmd = MLSD;
      XferList list = new XferList(su.path, du.path);
      path = list.sp;  // This will be normalized to end with /.

      // Check if we need to do a local listing.
      if (cc.sc.local)
        return StorkUtil.list(path);

      ChannelPair cc = new ChannelPair(this.cc.sc);

      LinkedList<String> dirs = new LinkedList<String>();
      dirs.add("");

      cc.rc.exchange("OPTS MLST type;size;");

      // Keep listing and building subdirectory lists.
      // TODO: Replace with pipelining structure.
      while (!dirs.isEmpty()) {
        LinkedList<String> subdirs = new LinkedList<String>();
        LinkedList<String> working = new LinkedList<String>();

        while (working.size() <= pipelining && !dirs.isEmpty())
          working.add(dirs.pop());

        // Pipeline commands like a champ.
        for (String p : working) {
          cc.pipePassive();
          cc.rc.write(cmd, path+p);
        }

        // Read the pipelined responses like a champ.
        for (String p : working) {
          ListSink sink = new ListSink(path);

          // Interpret the pipelined PASV command.
          try {
            HostPort hp = cc.getPasvReply();
            cc.setActive(hp);
          } catch (Exception e) {
            throw new Exception("couldn't set passive mode: "+e);
          }

          // Try to get the listing, ignoring errors unless it was root.
          try {
            cc.oc.facade.store(sink);
            cc.watchTransfer(null);
          } catch (Exception e) {
            e.printStackTrace();
            if (p.isEmpty())
              throw new Exception("couldn't list: "+path+": "+e);
            continue;
          }

          XferList xl = sink.getList(p);

          // If we did mlsr, return the list.
          if (cmd == MLSR)
            return xl;
          
          // Otherwise, add subdirs and repeat.
          for (XferList.Entry e : xl)
            if (e.dir) subdirs.add(e.path);
          list.addAll(xl);
        }

        // Get ready to repeat with new subdirs.
        dirs.addAll(subdirs);
      }

      return list;
    }

    // Get the size of a file.
    public long size(String path) throws Exception {
      if (cc.sc.local)
        return StorkUtil.size(path);
      Reply r = cc.sc.exchange("SIZE", path);
      if (!Reply.isPositiveCompletion(r))
        throw new Exception("file does not exist: "+path);
      return Long.parseLong(r.getMessage());
    }

    // Call this to kill transfer.
    public void abort() {
      for (ChannelPair cc : ccs)
        cc.abort();
      aborted = true;
    }

    // Check if we're prepared to transfer a file. This means we haven't
    // aborted and destination has been properly set.
    void checkTransfer() throws Exception {
      if (aborted)
        throw new Exception("transfer aborted");
    }

    // Do a transfer based on paths.
    void transfer(String sp, String dp) throws Exception {
      checkTransfer();

      XferList xl;

      // Some quick sanity checking.
      if (sp == null || sp.isEmpty())
        throw new Exception("src path is empty");
      if (dp == null || dp.isEmpty())
        throw new Exception("dest path is empty");

      System.out.println("Transferring: "+sp+" -> "+dp);

      // See if we're doing a directory transfer and need to build
      // a directory list.
      if (sp.endsWith("/")) {
        xl = mlsr(sp);
        xl.dp = dp;
      } else {  // Otherwise it's just one file.
        xl = new XferList(sp, dp, size(sp));
      }

      // Pass the list off to the transfer() which handles lists.
      transfer(xl);
    }

    // Transfer files from XferList.
    void transfer(XferList xl) throws Exception {
      checkTransfer();

      System.out.println("Transferring: "+xl.sp+" -> "+xl.dp);

      if (cc.dc.local || !cc.gridftp)
        cc.setTypeAndMode('I', 'S');
      else
        cc.setTypeAndMode('I', 'E'); 

      // Initialize optimizer.
      if (optimizer == null) {
        optimizer = new Optimizer();
      } else if (xl.size() >= 1E7) {
        optimizer.initialize(xl.size(), para_range);
      } else {
        System.out.println("File size < 1M, not optimizing...");
        optimizer = new Optimizer();
      } sink.mergeAd(new ClassAd("optimizer", optimizer.name()));

      // Connect source and destination server.
      HostPort hp = cc.setPassive();
      cc.setActive(hp);

      // mkdir dest directory.
      try {
        cc.pipeTransfer(xl.root);
        if (!cc.dc.local) cc.dc.read();
      } catch (Exception e) { /* who cares */ }

      // Let the progress monitor know we're starting.
      progress.transferStarted(xl.size(), xl.count());
      
      // Begin transferring according to optimizer.
      while (!xl.isEmpty()) {
        ClassAd b = optimizer.sample(), update;
        TransferProgress prog = new TransferProgress();
        int p = b.getInt("parallelism");
        int c = b.getInt("concurrency");
        long len = b.getLong("size");
        XferList xs;

        update = b.filter("parallelism", "concurrency");

        if (p > 0)
          setParallelism(p);
        else
          update.remove("parallelism");

        if (c > 0)
          setConcurrency(c);
        else
          update.remove("concurrency");

        if (sink != null && update.size() > 0)
          sink.mergeAd(update);

        if (len >= 0)
          xs = xl.split(len);
        else
          xs = xl.split(-1);

        prog.transferStarted(xs.size(), xs.count());
        transferList(xs);
        prog.transferEnded(true);

        b.insert("throughput", prog.throughputValue(true)/1E6);
        optimizer.report(b);
      }

      // Now let it know we've ended.
      progress.transferEnded(true);
    }

    // Split the list over all control channels and call
    // transferList(cc, xl).
    // FIXME: This will wait on the slowest thread.
    void transferList(XferList xl) throws Exception {
      checkTransfer();

      LinkedList<Thread> threads = new LinkedList<Thread>();
      long size = xl.size() / concurrency;

      if (concurrency <= 1 || size <= 0) {
        transferList(cc, xl);
        return;
      }

      System.out.println("Con. chunk size: "+size);

      // Generate lists for each other channel.
      for (int i = 1; i < concurrency; i++) {
        if (xl.isEmpty())
          break;

        final XferList li = xl.split(size);
        final ChannelPair c = ccs.get(i);

        System.out.println("Transferring on cc "+i+", size: "+li.size());

        // FIXME: Kinda ugly!
        Thread t = new Thread(new Runnable() {
          public void run() {
            try { transferList(c, li); }
            catch (Exception e) { /* :( */ }
          }
        });

        threads.add(t);
        t.start();
      }

      // Transfer the rest on the main channel.
      System.out.println("Transferring on cc 0, size: "+xl.size());
      //transferList(cc, xl);

      // Wait for all the threads to complete.
      for (Thread t: threads) t.join();
    }

    // Transfer a list over a channel.
    // TODO: Get rid of this adhoc pipelining.
    void transferList(ChannelPair cc, XferList xl) throws Exception {
      checkTransfer();

      System.out.println("Transferring list! size: "+xl.size());

      // Keep pipelining commands until we've transferred all bytes
      // or the list is empty.
      while (!xl.isEmpty()) {
        int p = pipelining;
        List<XferList.Entry> wl = new LinkedList<XferList.Entry>();

        // Pipeline p commands at a time, unless pipelining is -1,
        // in which case we have infinite pipelining.
        while (pipelining < 0 || p-- >= 0) {
          XferList.Entry e = xl.pop();

          if (e == null)
            break;
          System.out.println("Piping: "+e.path());

          cc.pipeTransfer(e);
          wl.add(e);
        }

        // Read responses to piped commands.
        for (XferList.Entry e : wl) {
          System.out.println("Getting: "+e.path());
          if (e.dir) try {
            if (!cc.dc.local) cc.dc.read();
          } catch (Exception ex) {
            // Who cares...
          } else {
            if (e.len < 0 && e.off > 0) {
              System.out.println("  - Reading...");
              if (!cc.sc.local) cc.sc.read();
              if (!cc.dc.local) cc.dc.read();
            }
            System.out.println("  - Watching...");
            cc.watchTransfer(progress);
            System.out.println("  - Done!");

            if (e.len < 0 || e.off + e.len >= e.size)
              progress.done(0, 1);
          } e.done = true;
        }
      }
    }
  }

  // Listens for markers from GridFTP servers and updates transfer
  // progress statistics accordingly.
  private static class ProgressListener implements MarkerListener {
    long last_bytes = 0;
    TransferProgress prog;

    public ProgressListener(TransferProgress prog) {
      this.prog = prog;
    }

    // When we've received a marker from the server.
    public void markerArrived(Marker m) {
      if (m instanceof PerfMarker) try {
        PerfMarker pm = (PerfMarker) m;
        long cur_bytes = pm.getStripeBytesTransferred();
        long diff = cur_bytes-last_bytes;

        last_bytes = cur_bytes;
        prog.done(diff);
      } catch (Exception e) {
        // Couldn't get bytes transferred...
      }
    }
  }

  // Transfer class
  // --------------
  static class GridFTPTransfer implements StorkTransfer {
    Thread thread = null;
    SubmitAd job;
    GSSCredential cred = null;

    StorkFTPClient client;
    FTPURI su = null, du = null;
    AdSink sink = new AdSink();

    volatile int rv = -1;
    volatile String message = null;

    GridFTPTransfer(SubmitAd job) {
      this.job = job;
    }

    public void process() throws Exception {
      String in = null;  // Used for better error messages.
      XferList xfer_list = null;

      // Check if we were provided a proxy. If so, load it.
      if (job.has("x509_proxy")) try {
        ExtendedGSSManager gm =
          (ExtendedGSSManager) ExtendedGSSManager.getInstance();
        cred = gm.createCredential(
          job.get("x509_proxy").getBytes(),
          ExtendedGSSCredential.IMPEXP_OPAQUE,
          GSSCredential.DEFAULT_LIFETIME, null,
          GSSCredential.INITIATE_AND_ACCEPT);
      } catch (Exception e) {
        fatal("error loading x509 proxy: "+e.getMessage());
      }

      // Bind URIs and cred into FTPURIs.
      try {
        in = "src";  su = new FTPURI(job.src, cred);
        in = "dest"; du = new FTPURI(job.dest, cred);
      } catch (Exception e) {
        fatal("error parsing "+in+": "+e.getMessage());
      }

      // Attempt to connect to hosts.
      // TODO: Differentiate between temporary errors and fatal errors.
      try {
        client = new StorkFTPClient(su, du);
      } catch (Exception e) {
        fatal("error connecting: "+e);
      }

      // Check that src and dest match.
      if (su.path.endsWith("/") && !du.path.endsWith("/"))
        fatal("src is a directory, but dest is not");

      // If parallelism was specified, see if it's a number or
      // contiguous range.
      // TODO: See if it's within a configured maximum.
      if (job.has("parallelism")) {
        Range p = Range.parseRange(job.get("parallelism"));
        if (p == null || !p.isContiguous())
          throw new Exception("parallelism must be a number or range");
        if (p.min() <= 0)
          throw new Exception("parallelism must be greater than zero");
        client.setParallelismRange(p.min(), p.max());
      }

      // We want this.
      client.setPerfFreq(1);

      // Check if concurrency was specified.
      if (job.has("concurrency"))
        client.setConcurrency(job.getInt("concurrency"));

      // Check if we should use an optimizer.
      // TODO: not this...
      if (job.has("optimizer")) {
        String name = job.get("optimizer");
        Optimizer opt = null;

        if (name.equals("full_2nd"))
          opt = new Full2ndOptimizer();
        else if (name.equals("full_c"))
          opt = new FullCOptimizer();
        else
          opt = new Optimizer();
        client.setOptimizer(opt);
      }

      client.setAdSink(sink);
      client.transfer(su.path, du.path);
    }

    private void abort() {
      if (client != null) try {
        client.abort();
      } catch (Exception e) { }

      close();
    }

    private void close() {
      try {
        if (client != null) client.close();
      } catch (Exception e) { }
    }

    public void run() {
      try {
        process();
        rv = 0;
      } catch (Exception e) {
        e.printStackTrace();
        ClassAd ad = new ClassAd();
        ad.insert("message", e.getMessage());
        sink.mergeAd(ad);
      }

      // Cleanup
      sink.close();
      //close();
    }

    public void fatal(String m) throws Exception {
      rv = 255;
      throw new Exception(m);
    }

    public void error(String m) throws Exception {
      rv = 1;
      throw new Exception(m);
    }

    public void start() {
      thread = new Thread(this);
      thread.start();
    }

    public void stop() {
      abort();
      sink.close();
      close();
    }

    public int waitFor() {
      if (thread != null) try {
        thread.join();
      } catch (Exception e) { }

      return (rv >= 0) ? rv : 255;
    }

    public ClassAd getAd() {
      return sink.getAd();
    }
  }

  // Methods
  // -------
  public ModuleInfoAd infoAd() { return info_ad; }

  public ClassAd validateAd(SubmitAd ad) throws Exception {
    return ad.filter(info_ad.opt_params);
  }

  public StorkTransfer transfer(SubmitAd ad) {
    return new GridFTPTransfer(ad);
  }

  // Tester
  // ------
  public static void main(String args[]) {
    SubmitAd ad = null;
    StorkGridFTPModule m = new StorkGridFTPModule();
    StorkTransfer tf = null;
    String optimizer = null;

    try {
      switch (args.length) {
        case 0:
          System.out.println("Enter a ClassAd:");
          ad = new SubmitAd(ClassAd.parse(System.in));
          break;
        case 1:
          ad = new SubmitAd(ClassAd.parse(new FileInputStream(args[0])));
          break;
        case 3:
          optimizer = args[2];
        case 2:
          ad = new SubmitAd(args[0], args[1]);
          break;
        default:
          System.out.printf("Usage: %s [src_url dest_url]\n", args[0]);
          System.exit(1);
      }
    } catch (Exception e) {
      System.out.println("Error: "+e.getMessage());
    }

    if (!ad.has("x509_proxy")) try {
      File f = new File("/tmp/x509up_u1000");
      Scanner s = new Scanner(f);
      StringBuffer sb = new StringBuffer();

      while (s.hasNextLine())
        sb.append(s.nextLine()+"\n");

      if (sb.length() > 0)
        ad.insert("x509_proxy", sb.toString());
    } catch (Exception e) {
      System.out.println("Couldn't open x509_file...");
    }

    ad.insert("optimizer", optimizer);

    try {
      ad.setModule(m);
    } catch (Exception e) {
      System.out.println("Error: "+e);
    }

    System.out.println("Starting...");
    tf = m.transfer(ad);
    tf.start();

    while (true) {
      ClassAd cad = tf.getAd();

      Runtime runtime = Runtime.getRuntime();
      System.out.println(runtime.freeMemory()+" free");

      if (cad != null)
        System.out.println("Got ad: "+cad);
      else break;
    }

    int rv = tf.waitFor();

    System.out.println("Job done with exit status "+rv);
  }

  public static void main2(String args[]) {
    URI uri;
    StorkFTPClient sc;

    try {
      // Parse URL
      uri = new URI(args[0]).normalize();

      // Open connection
      System.out.println("Reading credentials...");
      File cred_file = new File("/tmp/x509up_u1000");
      FileInputStream fis = new FileInputStream(cred_file);
      byte[] cred_bytes = new byte[(int) cred_file.length()];
      fis.read(cred_bytes);

      // Authenticate
      ExtendedGSSManager gm =
        (ExtendedGSSManager) ExtendedGSSManager.getInstance();
      GSSCredential cred = gm.createCredential(
          cred_bytes, ExtendedGSSCredential.IMPEXP_OPAQUE,
          GSSCredential.DEFAULT_LIFETIME, null,
          GSSCredential.INITIATE_AND_ACCEPT);

      System.out.println("Connecting to: "+uri);
      sc = new StorkFTPClient(new FTPURI(uri, cred), null);

      System.out.println("Listing...");
      XferList xl = sc.mlsr(uri.getPath());

      for (XferList.Entry e : xl)
        System.out.println(e.path());
    } catch (Exception e) {
      System.out.println("Error: "+e);
      e.printStackTrace();
    }
  }

  public static void main3(String[] args) {
    final TransferProgress tp = new TransferProgress();

    //tp.transferStarted(1000000000, 1);

    new Thread(new Runnable() {
      public void run() {
        try {
          double j = 0;
          for (double q = 0; q <= 1000; q = q+10) {
            double t = 500+j*(Math.random()-.5);
            double b = q*t;
            tp.done((long) b, 0);
            Thread.sleep((int) t);
          }
          System.out.println("Going to down...");
          Thread.sleep(1000);
          for (double q = 1000; q >= 0; q = q-10) {
            double t = 500+j*(Math.random()-.5);
            double b = q*t;
            tp.done((long) b, 0);
            Thread.sleep((int) t);
          }
          System.out.println("Going to 0...");
        } catch (Exception e) { }
      }
    }).start();

    while (true) try {
      System.out.println(tp.getAd());
      Thread.sleep(350);
    } catch (Exception e) { }
  }
}
