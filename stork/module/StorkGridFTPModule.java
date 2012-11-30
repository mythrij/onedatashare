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
      try { return br.readLine(); }
      catch (Exception e) { return null; }
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

  // A custom extended GridFTPClient that implements some undocumented
  // operations and provides some more responsive transfer methods.
  private static class StorkFTPClient extends GridFTPClient {
    public boolean gridftp = false;
    private StorkFTPClient dest = null;
    private TransferProgress progress = new TransferProgress();
    private AdSink sink = null;

    private int parallelism = 1, pipelining = 50, concurrency = 1;
    private Range para_range = new Range(1, 64);

    volatile boolean aborted = false;

    // Connect to a server without a credential.
    public StorkFTPClient(URI u) throws Exception {
      this(u, null);
    }

    // Connect to a server based on a URL and credential.
    public StorkFTPClient(URI u, GSSCredential cred) throws Exception {
      super(u.getHost(), port(u.getPort(), u.getScheme()));

      String ui = u.getUserInfo();
      String user = null, pass = null;

      setUsageInformation(info_ad.name, info_ad.version);

      // Parse out any user information.
      if (ui != null && !ui.isEmpty()) {
        int i = ui.indexOf(':');
        user = (i < 0) ? ui : ui.substring(0,i);
        pass = (i < 0) ? "" : ui.substring(i+1);
      }

      // Try to log in if we were given a username.
      if (cred != null)
        authenticate(cred, user);
      if (user != null)
        authorize(user, pass);
    }

    // Mungy way to "default-ize" a passed port in one statement.
    private static int port(int port, String p) throws Exception {
      if (p == null || p.isEmpty())
        throw new Exception("no protocol specified");
      if (p.equals("gridftp") || p.equals("gsiftp"))
        return (port > 0) ? port : 2811;
      else if (p.equals("ftp"))
        return (port > 0) ? port : 21;
      else if (p.equals("file"))
        return port;
      else
        throw new Exception("unsupported protocol: "+p);
    }

    // Set a server as this server's destination.
    public void setDest(StorkFTPClient d) throws Exception {
      dest = d;
      d.dest = this;
      dest.progress = progress;
    }

    // Set the progress listener for this client's transfers.
    public void setAdSink(AdSink sink) {
      this.sink = sink;
      progress.attach(sink);
    }

    public void setParallelism(int p) {
      p = (p < 1) ? 1 : p;
      parallelism = p;
      System.out.println("setParallelism("+p+")");
      try {
        controlChannel.execute(
          new Command("OPTS", "RETR Parallelism="+p+","+p+","+p+";"));
      } catch (Exception e) {
        System.out.println("Wasn't able to set parallelism to "+p+"...");
        e.printStackTrace();
      }
    }

    public void setParallelismRange(int lo, int hi) throws Exception {
      if (lo < 1 || hi < 1)
        throw new Exception("parallelism must be a positive value");
      para_range = new Range(lo, hi);
    }

    public void setPipelining(int p) {
      p = (p < 1) ? 1 : p;
      pipelining = p;
    }

    public void setConcurrency(int c) {
      c = (c < 1) ? 1 : c;
      concurrency = c;
    }

    // Some crazy undocumented voodoo magick!
    void setPerfFreq(int f) {
      try {
        Command cmd = new Command("TREV", "PERF "+f);
        controlChannel.execute(cmd);
      } catch (Exception e) { /* Well it was worth a shot... */ }
    }

    // Watch a transfer as it takes place, intercepting status messages
    // and reporting any errors. Use this for pipelined transfers.
    void watchTransfer(DataSink sink, boolean local) throws Exception {
      TransferState ss = new TransferState(), ds = new TransferState();
      BasicClientControlChannel cc = controlChannel, oc;
      TransferMonitor stm, dtm;
      ProgressListener pl = new ProgressListener(progress);

      if (sink != null)
        localServer.store(sink);

      if (local)
        oc = localServer.getControlChannel();
      else
        oc = dest.controlChannel;

      stm = new TransferMonitor(cc, ss, null, 50000, 2000, 1);
      dtm = new TransferMonitor(oc, ds, pl,   50000, 2000, 1);

      stm.setOther(dtm);
      dtm.setOther(stm);

      // Only watch the other control channel if it's not local.
      if (!local) {
        Thread t = new Thread(dtm);
        t.start();
        stm.run();
        t.join();
      } else {
        stm.run();
      }

      if (ss.hasError())
        throw ss.getError();
      if (ds.hasError())
        throw ds.getError();
    }

    // Read and handle the response of a pipelined PASV.
    public HostPort getPasvReply() throws Exception {
      Reply r = sread();
      String s = r.getMessage().split("[()]")[1];
      session.serverMode = Session.SERVER_PASSIVE;
      session.serverAddress = new HostPort(s);
      setLocalActive();
      return session.serverAddress;
    }

    // Recursively list directories.
    public XferList mlsr(String path) throws Exception {
      FTPControlChannel cc = controlChannel;
      final String MLSR = "MLSR", MLSD = "MLSD";
      String cmd = isFeatureSupported("MLSR") ? MLSR : MLSD;
      XferList list = new XferList(path, path);
      path = list.sp;  // This will be normalized to end with /.

      LinkedList<String> dirs = new LinkedList<String>();
      dirs.add("");

      try {
        cc.execute(new Command("OPTS MLST type;size;"));
      } catch (Exception e) { /* who cares */ }

      // Keep listing and building subdirectory lists.
      // TODO: Replace with pipelining structure.
      while (!dirs.isEmpty()) {
        LinkedList<String> subdirs = new LinkedList<String>();
        LinkedList<String> working = new LinkedList<String>();

        while (working.size() <= pipelining && !dirs.isEmpty())
          working.add(dirs.pop());

        // Pipeline commands like a champ.
        for (String p : working) {
          swrite(Command.PASV);
          swrite(cmd, path+p);
        }

        // Read the pipelined responses like a champ.
        for (String p : working) {
          ListSink sink = new ListSink(path);

          // Interpret the pipelined PASV command.
          try {
            getPasvReply();
          } catch (Exception e) {
            throw new Exception("couldn't set passive mode: "+e);
          }

          // Try to get the listing, ignoring errors unless it was root.
          try {
            watchTransfer(sink, true);
          } catch (Exception e) {
            if (p.isEmpty())
              throw new Exception("couldn't list: "+path);
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

    // Call this to kill transfer.
    public void abort() {
      try {
        swrite(Command.ABOR); 
        close();

        if (dest != null && dest != this) {
          dwrite(Command.ABOR); 
          dest.close();
        }
      } catch (Exception e) {
        // We don't care if aborting failed!
      } aborted = true;
    }

    // Check if we're prepared to transfer a file. This means we haven't
    // aborted and destination has been properly set.
    void checkTransfer() throws Exception {
      if (aborted)
        throw new Exception("transfer aborted");
      if (dest == null)
        throw new Exception("call setDest first");

      // TODO: Is this necessary? Efficient?
      checkGridFTPSupport();
      dest.checkGridFTPSupport();
      //gSession.matches(dest.gSession);
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
      } else try {  // Otherwise it's just one file.
        xl = new XferList(sp, dp, size(sp));
      } catch (Exception e) {
        xl = new XferList(sp, dp, -1);
      }

      // Pass the list off to the transfer() which handles lists.
      transfer(xl);
    }

    // Transfer files from XferList.
    void transfer(XferList xl) throws Exception {
      checkTransfer();

      System.out.println("Transferring: "+xl.sp+" -> "+xl.dp);

      setType(Session.TYPE_IMAGE);
      dest.setType(Session.TYPE_IMAGE);
      setMode(GridFTPSession.MODE_EBLOCK);
      dest.setMode(GridFTPSession.MODE_EBLOCK);

      Optimizer optimizer;

      // Initialize optimizer.
      if (xl.size() >= 1E7) {
        optimizer = new Full2ndOptimizer(xl.size(), para_range);
        sink.mergeAd(new ClassAd().insert("optimizer", "full_2nd"));
      } else {
        System.out.println("File size < 1M, not optimizing...");
        optimizer = new Optimizer();
        sink.mergeAd(new ClassAd().insert("optimizer", "none"));
      }

      // Connect source and destination server.
      HostPort hp = dest.setPassive();
      setActive(hp);

      // mkdir dest directory.
      try {
        sendPipedXfer(xl.root);
        recvPipedXferResp(xl.root);
      } catch (Exception e) { /* who cares */ }

      // Let the progress monitor know we're starting.
      progress.transferStarted(xl.size(), xl.count());
      
      // Begin transferring according to optimizer.
      while (!xl.isEmpty()) {
        ClassAd b = optimizer.sample();
        TransferProgress prog = new TransferProgress();
        int p = b.getInt("parallelism", 0);
        long len = b.getLong("size");
        XferList xs;

        // Set parameters
        if (p > 0) {
          setParallelism(p);
          if (sink != null)
            sink.mergeAd(new ClassAd("parallelism", p));
        }

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

    // Methods for sending raw commands to the source server.
    public void swrite(String... args) throws Exception {
      swrite(new Command(StorkUtil.join(args)));
    } public void swrite(Command cmd) throws Exception {
      controlChannel.write(cmd);
    }

    // Methods for sending raw commands to the dest server.
    public void dwrite(String... args) throws Exception {
      dwrite(new Command(StorkUtil.join(args)));
    } public void dwrite(Command cmd) throws Exception {
      dest.swrite(cmd);
    }

    // Method for receiving responses from the source server.
    public Reply sread() throws Exception {
      Reply r = controlChannel.read();
      return r;
    }

    // Method for receiving responses from the destination server.
    public Reply dread() throws Exception {
      return dest.sread();
    }

    // TODO: Refactor these.
    // Send command to transfer a file or make a directory down the pipe.
    void sendPipedXfer(XferList.Entry e) throws Exception {
      checkTransfer();

      FTPControlChannel cc = this.controlChannel, dc = dest.controlChannel;

      if (e.dir) {
        dwrite(new Command("MKD", e.dpath()));
      } else if (e.len > -1) {
        dwrite("ESTO", "A "+e.off+" "+e.dpath());
        swrite("ERET", "P "+e.off+" "+e.len+" "+e.path());
      } else {
        if (e.off > 0) {
          dwrite("REST", ""+e.off);
          swrite("REST", ""+e.off);
        }

        dwrite("STOR", e.dpath());
        swrite("RETR", e.path());
      }
    }

    // Receive the reponse to a piped file/mkdir command.
    void recvPipedXferResp(XferList.Entry e) throws Exception {
      checkTransfer();

      FTPControlChannel cc = this.controlChannel, dc = dest.controlChannel;

      if (e.dir) try {
        dread();  // Read and ignore MKD.
      } catch (Exception ex) {
        // Do nothing if we can't create directory.
      } else {
        if (e.len < 0 && e.off > 0) {
          dread(); sread();  // Read and ignore REST.
        } watchTransfer(null, false);

        if (e.len < 0 || e.off + e.len == e.size)
          progress.done(0, 1);
      }

    }

    // TODO: Develop some kind of pipelining mechanism.
    void transferList(XferList xl) throws Exception {
      checkTransfer();

      // Keep pipelining commands until we've transferred all bytes
      // or the list is empty.
      while (!xl.isEmpty()) {
        int p = pipelining;
        List<XferList.Entry> wl = new LinkedList<XferList.Entry>();

        // Pipeline p commands at a time, unless pipelining is -1,
        // in which case we have infinite pipelining.
        while (pipelining < 0 || p-- >= 0) {
          XferList.Entry x = xl.pop();

          if (x == null)
            break;

          sendPipedXfer(x);
          wl.add(x);
        }

        // Read responses to piped commands.
        for (XferList.Entry e : wl) {
          recvPipedXferResp(e);
          e.done = true;
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

        System.out.println("Got bytes: "+diff);

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

    StorkFTPClient sc, dc;
    URI su = null, du = null;
    AdSink sink = new AdSink();

    volatile int rv = -1;
    volatile String message = null;

    GridFTPTransfer(SubmitAd job) {
      this.job = job;
      su = job.src;
      du = job.dest;
    }

    public void process() throws Exception {
      String in = null;  // Used for better error messages.
      String sp = su.getPath(), dp = du.getPath();
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

      // Attempt to connect to hosts.
      // TODO: Differentiate between temporary errors and fatal errors.
      try {
        in = "src";  sc = new StorkFTPClient(su, cred);
        in = "dest"; dc = new StorkFTPClient(du, cred);
      } catch (Exception e) {
        fatal("couldn't connect to "+in+" server: "+e);
      }

      // If it's file-to-file, don't accept the job.
      // TODO: This.
      if (sc == null || dc == null) {
        //fatal("no FTP or GridFTP servers specified");
        fatal("currently only 3rd party transfers supported");
      }

      // TODO: If doing 3rd party, check if two servers can talk.

      // Hacky check if we're transferring to home directory.
      if (sp.startsWith("/~")) sp = sp.substring(1);
      if (dp.startsWith("/~")) dp = dp.substring(1);

      // Check that src and dest match.
      if (sp.endsWith("/") && !sp.endsWith("/"))
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
        sc.setParallelismRange(p.min(), p.max());
      }

      dc.setPerfFreq(1);
      sc.setDest(dc);
      sc.setAdSink(sink);
      sc.transfer(sp, dp);
    }

    private void abort() {
      try {
        if (sc != null) sc.abort();
        if (dc != null) dc.abort();
      } catch (Exception e) { }

      close();
    }

    private void close() {
      try {
        if (sc != null) sc.close(true);
        if (dc != null) dc.close(true);
      } catch (Exception e) { }
    }

    public void run() {
      try {
        process();
        rv = 0;
      } catch (Exception e) {
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

    try {
      switch (args.length) {
        case 0:
          System.out.println("Enter a ClassAd:");
          ad = new SubmitAd(ClassAd.parse(System.in));
          break;
        case 1:
          ad = new SubmitAd(ClassAd.parse(new FileInputStream(args[0])));
          break;
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
      System.out.println("Connecting to: "+uri);
      sc = new StorkFTPClient(uri);

      System.out.println("Reading credentials...");
      File cred_file = new File("/tmp/x509up_u1000");
      FileInputStream fis = new FileInputStream(cred_file);
      byte[] cred_bytes = new byte[(int) cred_file.length()];
      fis.read(cred_bytes);

      // Authenticate
      System.out.println("Authenticating...");
      ExtendedGSSManager gm =
        (ExtendedGSSManager) ExtendedGSSManager.getInstance();
      GSSCredential cred = gm.createCredential(
          cred_bytes, ExtendedGSSCredential.IMPEXP_OPAQUE,
          GSSCredential.DEFAULT_LIFETIME, null,
          GSSCredential.INITIATE_AND_ACCEPT);
      sc.authenticate(cred);

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
