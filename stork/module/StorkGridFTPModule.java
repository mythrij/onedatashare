package stork.module;

import stork.util.*;
import static stork.util.StorkUtil.Static.*;
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
    Ad ad = new Ad();
    ad.put("name", "Stork GridFTP Module");
    ad.put("version", "0.1");
    ad.put("author", "Brandon Ross");
    ad.put("email", "bwross@buffalo.edu");
    ad.put("description",
              "A rudimentary module for FTP and GridFTP transfers.");
    ad.put("protocols", "gsiftp,gridftp,ftp");
    ad.put("accepts", "classads");
    ad.put("opt_params", "parallelism,x509_proxy,optimizer");

    try {
      info_ad = new ModuleInfoAd(ad);
    } catch (Exception e) {
      info_ad = null;
      System.out.println("Fatal error parsing StorkGridFTPModule info ad");
      e.printStackTrace();
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
      System.out.println("Got buffer: "+new String(buffer.getBuffer()));
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
  static class ListSink implements DataSink {
    private XferList list;
    private int off = 0;
    private Reader reader;
    private StringBuffer sb;
    private List<String> newdirs;

    public ListSink(XferList list) throws Exception {
      sb = new StringBuffer();
      newdirs = new LinkedList<String>();
      this.list = list;
    }

    // Write a byte array of MLSD output to the sink.
    public synchronized void write(byte[] buf) {
      sb.append(new String(buf));
    }

    // Write a JGlobus buffer to the sink.
    public synchronized void write(Buffer buffer) throws IOException {
      write(buffer.getBuffer());
    }

    public void close() throws IOException { }

    // Get newly added directories.
    public synchronized List<String> getNewDirs() {
      return newdirs;
    }

    // Call this with the path after each listing completes. After
    // condensing, check newdirs for newly added directories.
    // FIXME: Maybe this isn't the best way to do this...
    public synchronized void condenseList(String dir) throws Exception {
      String line;

      newdirs.clear();
      StringReader sr = new StringReader(sb.toString());
      BufferedReader br = new BufferedReader(sr);

      // Read lines from the buffer list.
      while ((line = br.readLine()) != null) {
        try {
          MlsxEntry m = new MlsxEntry(line);

          String name = m.getFileName();
          String type = m.get("type");
          String size = m.get("size");

          if (type.equals(m.TYPE_FILE))
            list.add(dir+name, Long.parseLong(size));
          else if (!name.equals(".") && !name.equals(".."))
            newdirs.add(list.add(dir+name).path);
        } catch (Exception e) {
          e.printStackTrace();
          continue;  // Weird data I guess!
        }
      }

      // Clear stuff for the next listing.
      sb = new StringBuffer();
    }
  }

  // Wrapper for the JGlobus control channel classes which abstracts away
  // differences between local and remote transfers.
  private static class ControlChannel extends Pipeline<String, Reply> {
    public int port = -1;
    public final boolean local, gridftp;
    public final FTPServerFacade facade;
    public final FTPControlChannel fc;
    public final BasicClientControlChannel cc;
    private Set<String> features = null;
    private boolean cmd_done = false;

    public ControlChannel(FTPURI u) throws Exception {
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
          gc.authenticate(u.cred, u.user);
        } catch (Exception e) {
          throw E("could not authenticate (certificate issue?)");
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

        exchange("SITE CLIENTINFO appname="+info_ad.name+
                 ";appver="+info_ad.version+";schema=gsiftp;");
      } else {
        String user = (u.user == null) ? "anonymous" : u.user;
        cc = fc = new FTPControlChannel(u.host, u.port);
        fc.open();

        Reply r = exchange("USER "+user);
        if (Reply.isPositiveIntermediate(r)) try {
          execute("PASS "+u.pass);
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
          throw E("transfer failed to start");
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
    public void close() throws Exception {
      if (local)
        facade.close();
      else
        exchange("QUIT");
      flush();
      kill();
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

  // Class for binding a pair of control channels and performing pairwise
  // operations on them.
  private static class ChannelPair {
    public final FTPURI su, du;
    public final boolean gridftp;
    private int parallelism = 1, trev = 5, pipelining = 0;
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
        throw E("file-to-file not supported");
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

    // Pair a channel with a new local channel. Note: doesn't duplicate().
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
      //rc.exchange("TREV", "PERF", f);
      sc.exchange("OPTS RETR markers="+f+";");
    }

    // Flush both channels so they are synchronized.
    void sync() throws Exception {
      sc.flush(); dc.flush();
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
        throw E("no protocol specified");
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
        throw E("unsupported protocol: "+proto);
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
    private Range para_range = new Range(1, 64);
    private Optimizer optimizer = null;

    volatile boolean aborted = false;

    public StorkFTPClient(FTPURI su, FTPURI du) throws Exception {
      this.su = su; this.du = du;
      cc = new ChannelPair(su, du);
      ccs = new LinkedList<ChannelPair>();
      ccs.add(cc);
      setPipelining(0);
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
        throw E("parallelism must be a positive value");
      para_range = new Range(lo, hi);
    }

    public void setPipelining(int p) {
      for (ChannelPair c : ccs)
        c.setPipelining(p);
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

    // Recursively list directories. TODO: MLSC support.
    public XferList mlsr(String _path) throws Exception {
      final String MLSR = "MLSR ", MLSD = "MLSD ";
      final XferList list = new XferList(_path, "");
      final String path = list.sp;  // This will end with /.

      // Check if we need to do a local listing.
      if (cc.sc.local)
        return StorkUtil.list(path);

      final ChannelPair cc = new ChannelPair(this.cc.sc);
      final String cmd = MLSD;//cc.rc.supports("MLSR") ? MLSR : MLSD;

      // Turn of DCAU.
      if (cc.rc.supports("DCAU")) try {
        GridFTPServerFacade f = (GridFTPServerFacade) cc.oc.facade;
        f.setDataChannelAuthentication(DataChannelAuthentication.NONE);
        cc.rc.write("DCAU N", true);
      } catch (Exception e) {
        // Couldn't cast to GridFTPServerFacade probably, oh well.
      }

      final LinkedList<String> dirs = new LinkedList<String>();
      dirs.add("");

      cc.rc.write("OPTS MLST type;size;", true);

      // Create a sink which will write results into the list.
      final ListSink sink = new ListSink(list);

      // Keep listing and building subdirectory lists.
      while (!dirs.isEmpty()) {
        do {
          final String p = dirs.pop();

          // Set the server to passive mode to retrieve the list.
          cc.pipePassive();

          // Register a special handler and send list command.
          cc.dc.write(cmd+path+p, true, cc.dc.new XferHandler(null) {
            public Reply handleReply() throws Exception {
              cc.oc.facade.store(sink);
              Reply r = super.handleReply();

              sink.condenseList(p);
              if (cmd != MLSR)
                dirs.addAll(sink.getNewDirs());
              return r;
            }
          });
        } while (!dirs.isEmpty());
        cc.sync();
      }

      return list;
    }

    // Get the size of a file.
    public long size(String path) throws Exception {
      if (cc.sc.local)
        return StorkUtil.size(path);
      Reply r = cc.sc.exchange("SIZE "+path);
      if (!Reply.isPositiveCompletion(r))
        throw E("file does not exist: "+path);
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
        throw E("transfer aborted");
    }

    // Do a transfer based on paths.
    void transfer(String sp, String dp) throws Exception {
      checkTransfer();

      XferList xl;

      // Some quick sanity checking.
      if (sp == null || sp.isEmpty())
        throw E("src path is empty");
      if (dp == null || dp.isEmpty())
        throw E("dest path is empty");

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

      System.out.println("Transferring2: "+xl.sp+" -> "+xl.dp);

      System.out.println("Setting mode and type...");
      //if (cc.dc.local || !cc.gridftp)
      if (cc.dc.local)
        cc.setTypeAndMode('I', 'S');
      else
        cc.setTypeAndMode('I', 'E'); 

      // Initialize optimizer.
      System.out.println("Initializing optimizer...");
      if (optimizer == null) {
        optimizer = new Optimizer();
      } else if (xl.size() >= 1E7 || xl.size() <= 0) {
        optimizer.initialize(xl.size(), para_range);
      } else {
        System.out.println("File size < 1M, not optimizing...");
        optimizer = new Optimizer();
      } sink.mergeAd(new Ad("optimizer", optimizer.name()));

      // Connect source and destination server.
      System.out.println("Setting passive mode...");
      cc.pipePassive();

      // Make sure we were able to set passive mode.
      cc.rc.read();

      // mkdir dest directory.
      D("Piping root transfer...");
      cc.pipeXfer(xl.root, null);

      // Let the progress monitor know we're starting.
      progress.transferStarted(xl.size(), xl.count());
      
      // Begin transferring according to optimizer.
      while (!xl.isEmpty()) {
        Ad b = optimizer.sample(), update;
        TransferProgress prog = new TransferProgress();
        int s = b.getInt("pipelining");
        int p = b.getInt("parallelism");
        int c = b.getInt("concurrency");
        long len = b.getLong("size");
        XferList xs;

        System.out.println("Sample ad: "+b);
        System.out.println("Sample size: "+len);

        update = b.filter("pipelining", "parallelism", "concurrency");

        if (s > 0) setPipelining(s);
        else update.remove("pipelining");

        // FIXME: Parallelism and concurrency don't work well together.
        if (p > 0) {
          System.out.println("Got parallelism: "+p);
          if (p != parallelism) {
            cc.parallelism = p;
            cc.close();
            cc = cc.duplicate();
          } else setParallelism(p);
        } else {
          update.put("parallelism", parallelism);
        }

        if (c > 0) setConcurrency(c);
        else update.remove("concurrency");

        if (sink != null && update.size() > 0)
          sink.mergeAd(update);

        if (len >= 0)
          xs = xl.split(len);
        else
          xs = xl.split(-1);
        System.out.println("xl size: "+xl.size());
        System.out.println("xs size: "+xs.size());
        Thread.sleep(1000);

        prog.transferStarted(xs.size(), xs.count());
        transferList(xs);
        prog.transferEnded(true);

        b.put("throughput", prog.throughputValue(true)/1E6);
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
      transferList(cc, xl);

      // Wait for all the threads to complete.
      for (Thread t: threads) t.join();
    }

    // Transfer a list over a channel.
    void transferList(ChannelPair cc, XferList xl) throws Exception {
      checkTransfer();

      System.out.println("Transferring list! size: "+xl.size());

      while (!xl.isEmpty()) {
        //if (!extended mode) cc.pipePassive();
        cc.pipeXfer(xl.pop(), progress);
      } cc.sync();
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
        fatal("error connecting: "+e.getMessage());
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
          throw E("parallelism must be a number or range");
        if (p.min() <= 0)
          throw E("parallelism must be greater than zero");
        client.setParallelismRange(p.min(), p.max());
      } else {
        client.setParallelism(4);
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
        Ad ad = new Ad();
        ad.put("message", e.getMessage());
        sink.mergeAd(ad);
      }

      // Cleanup
      sink.close();
      //close();
    }

    public void fatal(String m) throws Exception {
      rv = 255;
      throw E(m);
    }

    public void error(String m) throws Exception {
      rv = 1;
      throw E(m);
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

    public Ad getAd() {
      return sink.getAd();
    }
  }

  // Methods
  // -------
  public ModuleInfoAd infoAd() { return info_ad; }

  public Ad validateAd(SubmitAd ad) throws Exception {
    return ad.filter(info_ad.opt_params);
  }

  public StorkTransfer transfer(SubmitAd ad) {
    return new GridFTPTransfer(ad);
  }

  // Tester
  // ------
  public static void main1(String args[]) {
    SubmitAd ad = null;
    StorkGridFTPModule m = new StorkGridFTPModule();
    StorkTransfer tf = null;
    String optimizer = null;

    try {
      switch (args.length) {
        case 0:
          System.out.println("Enter a Ad:");
          ad = new SubmitAd(Ad.parse(System.in));
          break;
        case 1:
          ad = new SubmitAd(Ad.parse(new FileInputStream(args[0])));
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
        ad.put("x509_proxy", sb.toString());
    } catch (Exception e) {
      System.out.println("Couldn't open x509_file...");
    }

    ad.put("optimizer", optimizer);

    try {
      ad.setModule(m);
    } catch (Exception e) {
      System.out.println("Error: "+e);
    }

    System.out.println("Starting...");
    tf = m.transfer(ad);
    tf.start();

    while (true) {
      Ad cad = tf.getAd();

      if (cad != null)
        System.out.println("Got ad: "+cad);
      else break;
    }

    int rv = tf.waitFor();

    System.out.println("Job done with exit status "+rv);
  }

  public static void main(String args[]) {
    URI uri, lri;
    StorkFTPClient sc;

    try {
      // Parse URL
      uri = new URI(args[0]).normalize();
      lri = new URI("file:///").normalize();

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
      sc = new StorkFTPClient(new FTPURI(uri, cred), new FTPURI(lri, null));

      System.out.println("Listing...");
      XferList xl = sc.mlsr(uri.getPath());

      System.out.println("List length: "+xl.size());
      for (XferList.Entry e : xl)
        System.out.println(e.path());
      System.out.println("Done listing!");
      for (Thread t : Thread.getAllStackTraces().keySet())
        System.out.println("Thread running: "+t);
      sc.close();
    } catch (Exception e) {
      System.out.println("Error: "+e);
      e.printStackTrace();
    }
  }

  public static void main3(String[] args) {
    final TransferProgress tp = new TransferProgress();

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
