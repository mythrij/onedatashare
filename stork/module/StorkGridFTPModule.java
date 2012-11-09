package stork.module;

import stork.util.*;
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
    ad.insert("opt_params",
              "parallelism,min_parallelism,max_parallelism,x509_proxy");

    try {
      info_ad = new ModuleInfoAd(ad);
    } catch (Exception e) {
      info_ad = null;
      System.out.println("Fatal error parsing StorkGridFTPModule info ad");
      System.exit(1);
    }
  }

  // Ad sink to allow for ads from multiple sources.
  private static class AdSink {
    volatile boolean closed = false;
    volatile boolean more = true;
    volatile ClassAd ad = null;

    public synchronized void close() {
      closed = true;
      System.out.println("Closing ad sink...");
      notifyAll();
    }

    public synchronized void putAd(ClassAd ad) {
      if (closed) return;
      this.ad = ad;
      notifyAll();
    }

    public synchronized void mergeAd(ClassAd a) {
      if (ad != null) putAd(ad.merge(a));
      else putAd(a);
    }
    
    // Block until an ad has come in, then clear the ad.
    public synchronized ClassAd getAd() {
      if (more) try {
        wait();
        if (closed) more = false;
        return ad;
      } catch (Exception e) {
        return null;
      } return null;
    }
  }

  // A list of transfers to be done by a pair of servers.
  private static class XferList implements Iterable<XferEntry> {
    List<XferEntry> paths;
    int num_done = 0, num_total = 0;
    long bytes_done = 0, bytes_total = 0;

    XferList() {
      paths = new LinkedList<XferEntry>();
    }

    // Add a directory.
    void add(String path) {
      paths.add(new XferEntry(path, -1));
    }

    // Add a file with a given size.
    void add(String path, long size) {
      paths.add(new XferEntry(path, size));
      if (size > 0)
        bytes_total += size;
      num_total++;
    }

    // Merge an XferList into this one.
    void addAll(XferList xl) {
      paths.addAll(xl.paths);
      num_done += xl.num_done;
      num_total += xl.num_total;
      bytes_done += xl.bytes_done;
      bytes_total += xl.bytes_total;
    }

    // Mark a transfer entry as done.
    void done(XferEntry te) {
      if (te.done) return;
      te.done = true;
      if (te.size > 0)
        bytes_done += te.size;
      num_done++;
    }

    public Iterator<XferEntry> iterator() {
      return paths.iterator();
    }
  }

  private static class XferEntry {
    final String path;
    final long size;
    boolean done = false;

    XferEntry(String p, long sz) {
      path = p; size = sz;
    }
  }

  // A custom extended GridFTPClient that implements some undocumented
  // operations and provides some more responsive transfer methods.
  private static class StorkFTPClient extends GridFTPClient {
    public boolean gridftp = false;
    private StorkFTPClient dest = null;

    private int parallelism = 1, pipelining = 50, concurrency = 1;

    volatile boolean aborted = false;
    Exception abort_exception = new Exception("transfer aborted");

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
    public void setDest(StorkFTPClient d) {
      dest = d;
    }

    // Some crazy undocumented voodoo magick!
    void setPerfFreq(int f) {
      try {
        Command cmd = new Command("TREV", "PERF "+f);
        controlChannel.execute(cmd);
      } catch (Exception e) { /* Well it was worth a shot... */ }
    }

    // A sink meant to receive MLSD lists. It contains a list of
    // JGlobus Buffers (byte buffers with offsets) that it reads
    // through sequentially using a BufferedReader to read lines
    // and parse data returned by FTP and GridFTP MLSD commands.
    private class ListSink extends Reader implements DataSink {
      private LinkedList<Buffer> buf_list;
      private Buffer cur_buf = null;
      private BufferedReader br;
      private int off = 0;

      public ListSink() {
        buf_list = new LinkedList<Buffer>();
        br = new BufferedReader(this);
      }

      public void write(Buffer buffer) throws IOException {
        System.out.println("Buffer: "+new String(buffer.getBuffer()));
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

      // Get the list from the sink.
      public XferList getList(String path) {
        XferList xl = new XferList();
        String line;

        // Read lines from the buffer list.
        while ((line = readLine()) != null) {
          try {
            MlsxEntry m = new MlsxEntry(line);

            String name = m.getFileName();
            String type = m.get("type");
            String size = m.get("size");

            if (type.equals(m.TYPE_FILE))
              xl.add(path+name, Long.parseLong(size));
            else if (!name.equals(".") && !name.equals(".."))
              xl.add(path+name+"/");
          } catch (Exception e) {
            e.printStackTrace();
            continue;  // Weird data I guess!
          }
        } return xl;
      }
    }

    // Recursively list directories.
    public XferList mlsr(String path) throws Exception {
      FTPControlChannel cc = controlChannel;
      XferList list = new XferList();
      boolean has_mlsr = isFeatureSupported("MLSR");

      if (!path.endsWith("/")) path += "/";

      LinkedList<String> dirs = new LinkedList<String>();
      dirs.add(path);

      try {
        cc.execute(new Command("OPTS MLST type;size;"));
      } catch (Exception e) { /* who cares */ }

      // Early passive mode
      cc.write(Command.PASV);
      try {
        Reply r = controlChannel.read();
        String s = r.getMessage().split("[()]")[1];
        session.serverMode = Session.SERVER_PASSIVE;
        session.serverAddress = new HostPort(s);
        setLocalActive();
      } catch (Exception e) {
        throw new Exception("couldn't set passive mode for MLSR: "+e);
      }

      // Keep listing and building subdirectory lists.
      while (!dirs.isEmpty()) {
        LinkedList<String> subdirs = new LinkedList<String>();
        LinkedList<String> working = new LinkedList<String>();

        System.out.println("building working list");
        while (working.size() < pipelining && !dirs.isEmpty())
          working.add(dirs.pop());

        // Pipeline commands like a champ.
        System.out.println("piping commands");
        for (String p : working) {
          //cc.write(Command.PASV);
          cc.write(new Command(has_mlsr ? "MLSR" : "MLSD", p));
        }

        // Read the pipelined responses like a champ.
        System.out.println("reading piped responses");
        for (String p : working) {
          ListSink sink = new ListSink();
          TransferState ts = new TransferState();
          TransferMonitor tm;

          // Interpret the pipelined PASV command.
          /*
          try {
            Reply r = controlChannel.read();
            String s = r.getMessage().split("[()]")[1];
            session.serverMode = Session.SERVER_PASSIVE;
            session.serverAddress = new HostPort(s);
            setLocalActive();
          } catch (Exception e) {
            throw new Exception("couldn't set passive mode for MLSR: "+e);
          }
          */

          // Perform the transfer.
          localServer.store(sink);

          tm = new TransferMonitor(cc, ts, null, 50000, 2000, 1);
          tm.setOther(tm);
          tm.run();

          if (ts.hasError()) continue;

          XferList xl = sink.getList(p);

          // If we did mlsr, return the list. Else, repeat.
          if (has_mlsr)
            return xl;
          else for (XferEntry e : xl)
            if (e.size == -1) subdirs.add(e.path);

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
        controlChannel.write(Command.ABOR); 
        if (dest != null && dest != this)
          dest.controlChannel.write(Command.ABOR); 
      } catch (Exception e) {
        // We don't care if aborting failed!
      }
      aborted = true;
    }

    // Transfer a whole list of files.
    void transfer(XferList xl, ProgressListener pl) throws Exception {
      if (aborted)
        throw abort_exception;

      if (dest == null)
        throw new Exception("call setDest first");

      checkGridFTPSupport();
      dest.checkGridFTPSupport();

      gSession.matches(dest.gSession);

      // First pipeline all the commands.
      for (XferEntry x : xl) {
        if (aborted)
          throw abort_exception;

        // We have to make a directory.
        if (x.size == -1) {
          Command cmd = new Command("MKD", x.path);
          dest.controlChannel.write(cmd);
        }

        // We have to transfer a file.
        else {
          Command dcmd = new Command("STOR", x.path);
          dest.controlChannel.write(dcmd);

          Command scmd = new Command("RETR", x.path);
          controlChannel.write(scmd);
        }
      }

      // Then watch the file transfers.
      for (XferEntry x : xl) {
        if (aborted)
          throw abort_exception;
        if (x.size == -1)  // Read and ignore mkdir results.
          dest.controlChannel.read();
        else
          transferRunSingleThread(dest.controlChannel, pl);
        xl.done(x);
        if (pl != null)
          pl.fileComplete(x);
      } pl.transferComplete();
    }
  }

  private static class ProgressListener implements MarkerListener {
    volatile boolean done = false, reallydone = false;
    ClassAd ad = new ClassAd();
    int num_done = 0, num_total = 0;
    long last_bytes = 0;
    XferList list = null;
    AdSink sink;
    TransferProgress prog;

    // Single file transfer.
    public ProgressListener(AdSink sink, long total) {
      this.sink = sink;
      prog = new TransferProgress();
      prog.setBytes(total);
      prog.transferStarted();
    }

    // Multiple file transfer.
    public ProgressListener(AdSink sink, XferList list) {
      this.sink = sink;
      this.list = list;
      prog = new TransferProgress();
      prog.setBytes(list.bytes_total);
      prog.setFiles(list.num_total);
      prog.transferStarted();
    }

    // When we've received a marker from the server.
    public void markerArrived(Marker m) {
      if (m instanceof PerfMarker) try {
        PerfMarker pm = (PerfMarker) m;
        long cur_bytes = pm.getStripeBytesTransferred();
        long diff = cur_bytes-last_bytes;

        last_bytes = cur_bytes;

        if (diff >= 0)
          prog.bytesDone(diff);
      } catch (Exception e) {
        // Couldn't get progress from marker...
      } else if (m instanceof RestartMarker) {
        // TODO: Save last restart marker
      }

      updateAd();
    }

    // Called when a file is done in a multifile transfer.
    public void fileComplete(XferEntry e) {
      long diff = e.size - last_bytes;

      fileComplete(diff);

      // Reset for next file
      last_bytes = 0;
    }

    public void fileComplete(long size) {
      if (size >= 0)
        prog.fileDone(size);
      updateAd();
    }

    public void transferComplete() {
      prog.transferEnded();
    }

    private void message(String m) {
      ad.insert("message", m);
      sink.mergeAd(ad);
    }

    // Call done(false) to check, call done(true) to stop. After calling
    // done(true), done(false) will return false one last time allowing 
    // the loop to execute one last time to check for final progress.
    synchronized boolean done(boolean d) {
      if (d)   { return done = true; }
      else try { return reallydone; }
      finally  { reallydone = done; }
    } synchronized boolean done() {
      return done(false);
    }

    private void updateAd() { 
      ad.insert("byte_progress", prog.byteProgress());
      ad.insert("progress", prog.progress());
      ad.insert("file_progress", prog.fileProgress());
      ad.insert("throughput", prog.throughput(false));
      ad.insert("avg_throughput", prog.throughput(true));

      sink.mergeAd(ad);
    }
  };

  // Transfer class
  // --------------
  static class GridFTPTransfer implements StorkTransfer {
    Thread thread = null;
    SubmitAd job;
    GSSCredential cred = null;

    StorkFTPClient sc, dc;
    URI su = null, du = null;
    ProgressListener pl = null;
    AdSink sink = new AdSink();

    volatile int rv = -1;
    volatile String message = null;

    GridFTPTransfer(SubmitAd job) {
      this.job = job;
    }

    // Set the return value and message atomically.
    private synchronized void set(int r, String m) {
      rv = r;
      message = m;
      sink.mergeAd(new ClassAd().insert("message", m));
      if (r >= 0) sink.close();
    }

    public void process() throws Exception {
      String in = null;  // Used for better error messages.
      XferList xfer_list = null;

      // Make sure we have a src and dest url.
      su = job.src;
      du = job.dest;

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
      if (sc == null || dc == null) {
        //fatal("no FTP or GridFTP servers specified");
        fatal("currently only 3rd party transfers supported");
      }

      // TODO: If doing 3rd party, check if two servers can talk.

      // Generate a transfer list from the source URL if directory,
      // and init progress listener based on list.
      if (su.getPath().endsWith("/")) try {
        xfer_list = sc.mlsr(su.getPath());

        pl = new ProgressListener(sink, xfer_list);

        // Make sure dest_url is a directory...
        if (!du.getPath().endsWith("/")) {
          fatal("src is a directory, but dest is not");
          return;
        }

        if (sc != null) sc.changeDir(su.getPath());
        if (dc != null) try {
          dc.changeDir(du.getPath());
        } catch (Exception e) {  // Path does not exist, so create.
          dc.makeDir(du.getPath());
          dc.changeDir(du.getPath());
        }
      } catch (Exception e) {
        fatal("error generating transfer list: "+e);
      }

      // If it's not a directory, it's a file. Get its size and init
      // the progress listener.
      else try {
        long size = sc.size(su.getPath());
        pl = new ProgressListener(sink, size);
        dc.setPerfFreq(1);

        // Check if destination is a directory.
        if (dc != null && du.getPath().endsWith("/")) try {
          dc.changeDir(du.getPath());
        } catch (Exception e) {  // Path does not exist, so create.
          dc.makeDir(du.getPath());
          dc.changeDir(du.getPath());
        } finally {
          du = du.relativize(su);
          System.out.println("relativized du: "+du);  // Debugging
        }
      } catch (Exception e) {
        fatal("error getting file size for source file");
      }

      // Set options according to job ad
      if (job.has("parallelism")) {
        final int def = job.getInt("parallelism", 1),
                  min = job.getInt("min_parallelism", def),
                  max = job.getInt("max_parallelism", def);

        if (def < 1 || min < 1 || max < 1)
          fatal("parallelism levels must be greater than zero");
        if (min > def || def > max)
          fatal("inconsistency in parallelism constraints");

        Options o = new Options("RETR") {
          int a = def, b = min, c = max;
          public String getArgument() {
            return String.format("Parallelism=%d,%d,%d;", a, b, c);
          }
        };

        // Try to set parallelism levels.
        try {
          if (sc != null) sc.setOptions(o);
          if (dc != null) dc.setOptions(o);
        } catch (Exception e) {
          fatal("error setting parallelism level");
        }
      }

      // Put servers into state to prepare for transfer.
      try {
        in = "src";  if (sc != null) sc.setType(Session.TYPE_IMAGE);
        in = "dest"; if (dc != null) dc.setType(Session.TYPE_IMAGE);
      } catch (Exception e) {
        fatal("couldn't change "+in+" server mode: "+e.getMessage());
      }

      // If we're doing multifile, transfer all files.
      if (xfer_list != null) try {
        sc.setMode(GridFTPSession.MODE_EBLOCK);
        dc.setMode(GridFTPSession.MODE_EBLOCK);

        // Bulk transfer
        sc.setDest(dc);
        sc.setActive(dc.setPassive());
        sc.transfer(xfer_list, pl);
      } catch (Exception e) {
        fatal("couldn't transfer: "+e.getMessage());
      }

      // Otherwise it's a single file transfer.
      else try {
        sc.setMode(GridFTPSession.MODE_EBLOCK);
        dc.setMode(GridFTPSession.MODE_EBLOCK);

        sc.transfer(su.getPath(), dc, du.getPath(), false, pl);
        pl.fileComplete(0);
        pl.transferComplete();
      } catch (Exception e) {
        fatal("couldn't transfer: "+e.getMessage());
      }
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
        if (sc != null) sc.close();
        if (dc != null) dc.close();
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
      close();
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
  public static void main2(String args[]) {
    ClassAd ad = null;
    StorkGridFTPModule m = new StorkGridFTPModule();
    StorkTransfer tf = null;

    try {
      switch (args.length) {
        case 0:
          System.out.println("Enter a ClassAd:");
          tf = m.transfer(new SubmitAd(ClassAd.parse(System.in)));
          break;
        case 2:
          tf = m.transfer(args[0], args[1]);
          break;
        default:
          System.out.printf("Usage: %s [src_url dest_url]\n", args[0]);
          System.exit(1);
      }
    } catch (Exception e) {
      System.out.println("Error: "+e.getMessage());
    }

    System.out.println("Starting...");
    tf.start();

    while (true) {
      ad = tf.getAd();

      if (ad != null)
        System.out.println("Got ad: "+ad);
      else break;
    }

    int rv = tf.waitFor();

    System.out.println("Job done with exit status "+rv);
  }

  public static void main(String args[]) {
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

      for (XferEntry e : xl)
        System.out.println(e.path);
    } catch (Exception e) {
      System.out.println("Error: "+e);
      e.printStackTrace();
    }
  }

  public static void main4(String args[]) {
    double thrp;  // MB/s
    TransferProgress prog = new TransferProgress();

    try {
      thrp = Double.parseDouble(args[0]);
    } catch (Exception e) {
      thrp = 50;
    }

    prog.setBytes(1000000000l);
    prog.transferStarted();

    while (true) try {
      prog.bytesDone(50*1000*100);
      System.out.println("inst = "+prog.throughput(false));
      System.out.println("avg  = "+prog.throughput(true));
      System.out.println("bprg = "+prog.byteProgress());
      System.out.println("prg  = "+prog.progress());
      Thread.sleep(100);
      System.out.println();
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
  }
}
