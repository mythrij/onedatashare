package stork.module;

import stork.util.*;
import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

public class StorkGridFTPModule extends TransferModule {
  private static final ClassAd info_ad = new ClassAd();

  private static final String pstr = "gridftp,gftp,ftp";
  private static final String[] protocols = splitProtocols(pstr);

  private static final String name = "Stork GridFTP Module";
  private static final String version = "0.1";

  static {
    info_ad.insert("name", name);
    info_ad.insert("version", version);
    info_ad.insert("author", "Brandon Ross");
    info_ad.insert("email", "bwross@buffalo.edu");
    info_ad.insert("protocols", pstr);
    info_ad.insert("accepts", "classads");
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

    XferList(URI s, URI d) {
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
    String path;
    long size;
    boolean done = false;

    XferEntry(String p, long sz) {
      path = p; size = sz;
    }
  }

  // A custom extended GridFTPClient for multifile transfers.
  private static class StorkGFTPClient extends GridFTPClient {
    private String[] features = null;
    StorkGFTPClient sc, dc;
    volatile boolean aborted = false;
    Exception abort_exception = new Exception("transfer aborted");

    StorkGFTPClient(String host, int port) throws Exception {
      super(host, port);
      sc = dc = this;
    }

    void setDest(StorkGFTPClient d) { dc = d; }
    void setSrc(StorkGFTPClient s)  { sc = s; }

    // Some crazy undocumented voodoo magick!
    void setPerfFreq(int f) {
      try {
        Command cmd = new Command("TREV", "PERF "+f);
        controlChannel.execute(cmd);
      } catch (Exception e) {
        // Well it was worth a shot...
      }
    }

    // Stupid that this is private in FTPClient...
    private class ByteDataSink implements DataSink {
      private ByteArrayOutputStream received;

      public ByteDataSink() {
        this.received = new ByteArrayOutputStream(1000);
      }

      public void write(Buffer buffer) throws IOException {
        this.received.write(buffer.getBuffer(), 0, buffer.getLength());
      }

      public void close() throws IOException { }

      public ByteArrayOutputStream getData() {
        return this.received;
      }
    }

    // GridFTP extension which does recursive listings.
    public Vector mlsr(String path) throws Exception {
      ByteDataSink sink = new ByteDataSink();
      Command cmd;

      // Set MLSR options.
      try {
        controlChannel.execute(new Command("OPTS", "MLST type;size;"));
        //controlChannel.execute(new Command("OPTS", "MLSR onerror=continue"));
      } catch (Exception e) {
        System.out.println("OPTS: "+e);
      }

      cmd = new Command("MLSR", path);
      performTransfer(cmd, sink);

      ByteArrayOutputStream received = sink.getData();

      BufferedReader reader =
        new BufferedReader(new StringReader(received.toString()));

      System.out.println("Stuff: "+received);

      Vector<MlsxEntry> list = new Vector<MlsxEntry>();
      XferEntry entry = null;
      String line = null;

      while ((line = reader.readLine()) != null)
        list.addElement(new MlsxEntry(line));
      return list;
    }


    // Call this to kill transfer.
    public void abort() {
      try {
        sc.controlChannel.write(Command.ABOR); 
        if (sc != dc)
          dc.controlChannel.write(Command.ABOR); 
      } catch (Exception e) {
        // We don't care if aborting failed!
      }
      aborted = true;
    }

    void transfer(XferList xl, ProgressListener pl) throws Exception {
      if (aborted)
        throw abort_exception;

      sc.checkGridFTPSupport();
      dc.checkGridFTPSupport();

      sc.gSession.matches(dc.gSession);

      // First pipeline all the commands.
      for (XferEntry x : xl) {
        if (aborted)
          throw abort_exception;

        // We have to make a directory.
        if (x.size == -1) {
          Command cmd = new Command("MKD", x.path);
          dc.controlChannel.write(cmd);
        }

        // We have to transfer a file.
        else {
          Command dcmd = new Command("STOR", x.path);
          dc.controlChannel.write(dcmd);

          Command scmd = new Command("RETR", x.path);
          sc.controlChannel.write(scmd);
        }
      }

      // Then watch the file transfers.
      for (XferEntry x : xl) {
        if (aborted)
          throw abort_exception;
        if (x.size == -1)  // Read and ignore mkdir results.
          dc.controlChannel.read();
        else
          sc.transferRunSingleThread(dc.controlChannel, pl);
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
    ClassAd job;
    GSSCredential cred = null;

    FTPClient sc = null, dc = null;
    URI su = null, du = null;
    ProgressListener pl = null;
    AdSink sink = new AdSink();

    volatile int rv = -1;
    volatile String message = null;

    GridFTPTransfer(ClassAd job) {
      this.job = job;
    }

    // Set the return value and message atomically.
    private synchronized void set(int r, String m) {
      rv = r;
      message = m;
      sink.mergeAd(new ClassAd().insert("message", m));
      if (r >= 0) sink.close();
    }

    // Try to connect to a given URL. If cred isn't null, attempt
    // to authenticate if it's a GridFTP server. Returns a GridFTPClient
    // or FTPClient for gridftp and ftp respectively, null if it's a
    // file, or throws an exception if either it's an unsupported
    // protocol or there was a problem connecting.
    private FTPClient connect(URI u, GSSCredential cred) throws Exception {
      FTPClient cli = null;
      StorkGFTPClient gcli = null;

      String p = u.getScheme();
      String host = u.getHost();
      int port = u.getPort();

      String ui = u.getUserInfo();
      String user = null, pass = null;

      if (p == null)
        throw new Exception("protocol not specified in URL");

      // Parse out any user information.
      if (ui != null && !ui.isEmpty()) {
        int i = ui.indexOf(':');
        user = (i < 0) ? ui : ui.substring(0,i);
        pass = (i < 0) ? "" : ui.substring(i+1);
      }

      // Try to connect to the (Grid)FTP server.
      if (p.equals("gridftp") || p.equals("gsiftp") || p.equals("gftp"))
        cli = gcli = new StorkGFTPClient(host, (port > 0) ? port : 2811);
      else if (p.equals("ftp"))
        cli = new FTPClient(host, (port > 0) ? port : 21);
      else if (p.equals("file"))
        return null;
      else throw new Exception("unsupported protocol: "+p);

      // If we're using GridFTP and got a cred, try to auth with that.
      if (gcli != null && cred != null) {
        gcli.authenticate(cred, user);
        //gcli.setMode(GridFTPSession.MODE_EBLOCK);
      }

      // Otherwise, try username/password if given.
      else if (user != null)
        cli.authorize(user, pass);

      cli.setType(Session.MODE_BLOCK);

      return cli;
    } 

    // Build a transfer list recursively given a source URL. The strings
    // in the returned array should be meaningful relative to u.
    private XferList xfer_list(String p) throws Exception {
      String path = su.getPath()+p;
      boolean directory = path.endsWith("/"),
              wildcard  = path.indexOf('*') >= 0,
              recursive = directory || wildcard;
      XferList list = new XferList(su, du);

      // If there's no source client, files come from local machine.
      // TODO: Wildcards.
      /*
      if (sc == null) {
        File file = new File(u).getAbsoluteFile();
        File[] files = file.listFiles();

        // File is not a directory, no need to recurse.
        if (files == null) {
          if (file.isFile())
            return new String[] { file.getName() };
          else  // Character device or something... ignore.
            return new String[0];
        }

        // It's a directory, but maybe we don't want to recurse.
        if (!recursive) return new String[0];

        // Recursively build string list. Memory inefficient! :(
        String base = file.getName()+"/";
        strings.add(base);

        for (File f : files) {
          if (f.isFile())  // Don't recurse, it'd be a waste.
            strings.add(base+f.getName());
          else if (!f.isDirectory())
            continue;
          else for (String s : xfer_list(f.toURI()))
            strings.add(base+s);
        }

        return (String[]) strings.toArray();
      }
      */

      // Otherwise, it's a third party transfer. Files come from server.
      // Is there a way we can do this without making so many calls?
      MlsxEntry me = sc.mlst(path);
      String type = me.get("type");
      String size = me.get("size");

      if (me == null || type == null || size == null)
        return list;

      if (type.equals(me.TYPE_FILE)) {
        list.add(p, Long.parseLong(size));
        return list;
      }

      // Ignore weird file types.
      if (!type.equals(me.TYPE_DIR))
        return list;

      // TODO: This...
      if (wildcard) {
        return list;
      }

      // Try to list contents with MLSR.
      if (directory && sc instanceof StorkGFTPClient) try {
        StorkGFTPClient ssc = (StorkGFTPClient) sc;
        for (Object o : ssc.mlsr(path)) if (o instanceof MlsxEntry) {
          MlsxEntry m = (MlsxEntry) o;
          String name = m.getFileName();
          type = me.get("type");
          size = me.get("size");

          if (m.get("type").equals(m.TYPE_FILE))
            list.add(p+name, Long.parseLong(size));
          else if (!me.get("type").equals(me.TYPE_DIR))
            continue;
          else
            list.add(p+name+"/");
        } return list;
      } catch (Exception e) {
        // Don't support MLSR. :(
      }

      // It's a directory, we have to recurse.
      if (directory) {
        sc.setPassiveMode(true);
        Object[] mes = sc.mlsd(path).toArray();
        if (!p.isEmpty()) list.add(p);

        for (Object o : mes) if (o instanceof MlsxEntry) {
          MlsxEntry m = (MlsxEntry) o;
          String name = m.getFileName();
          type = me.get("type");
          size = me.get("size");

          if (m.get("type").equals(m.TYPE_FILE))
            list.add(p+name, Long.parseLong(size));
          else if (!me.get("type").equals(me.TYPE_DIR))
            continue;
          else if (name.equals(".") || name.equals(".."))
            continue;
          else
            list.addAll(xfer_list(p+name+"/"));
        }
      }

      return list;
    }

    public void process() throws Exception {
      String in = null;  // Used for better error messages.
      XferList xfer_list = null;

      // Make sure we have a src and dest url.
      try {
        su = new URI(job.get(in = "src_url"));
        du = new URI(job.get(in = "dest_url"));
      } catch (Exception e) {
        fatal("couldn't parse "+in+": "+e.getMessage());
      }

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
        in = "src";  sc = connect(su, cred);
        in = "dest"; dc = connect(du, cred);
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
        xfer_list = xfer_list("");

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
        ((StorkGFTPClient)dc).setPerfFreq(1);

        // Check if destination is a directory.
        if (dc != null && du.getPath().endsWith("/")) try {
          dc.changeDir(du.getPath());
        } catch (Exception e) {  // Path does not exist, so create.
          dc.makeDir(du.getPath());
          dc.changeDir(du.getPath());
        } finally {
          du = du.relativize(su);
        }
      } catch (Exception e) {
        System.out.println("Couldn't make progress listener for file...");
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
        if (dc instanceof StorkGFTPClient && sc instanceof StorkGFTPClient) {
          StorkGFTPClient gsc = (StorkGFTPClient) sc,
                          gdc = (StorkGFTPClient) dc;

          gsc.setMode(GridFTPSession.MODE_EBLOCK);
          gdc.setMode(GridFTPSession.MODE_EBLOCK);

          // Bulk transfer
          gsc.setDest(gdc);
          gsc.setActive(gdc.setPassive());
          gsc.transfer(xfer_list, pl);
        }
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
  public ClassAd info_ad() { return info_ad; }
  public String[] protocols() { return protocols; }
  public String name() { return name; }
  public String version() { return version; }

  public StorkTransfer transfer(ClassAd ad) {
    return new GridFTPTransfer(ad);
  }

  // Tester
  // ------
  public static void main2(String args[]) {
    ClassAd ad = null;
    StorkGridFTPModule m = new StorkGridFTPModule();
    StorkTransfer tf = null;

    switch (args.length) {
      case 0:
        System.out.println("Enter a ClassAd:");
        tf = m.transfer(ClassAd.parse(System.in));
        break;
      case 2:
        tf = m.transfer(args[0], args[1]);
        break;
      default:
        System.out.printf("Usage: %s [src_url dest_url]\n", args[0]);
        System.exit(1);
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

  public static void main3(String args[]) {
    URI uri;
    StorkGFTPClient sc;
    int port = 2811;

    try {
      // Parse URL
      uri = new URI(args[0]);

      // Open connection
      System.out.println("Connecting to: "+uri);
      if (uri.getPort() > 0) port = uri.getPort();
      sc = new StorkGFTPClient(uri.getHost(), port);

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
      sc.mlsr(uri.getPath());
    } catch (Exception e) {
      System.out.println("Error: "+e);
      e.printStackTrace();
    }
  }

  public static void main(String args[]) {
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
