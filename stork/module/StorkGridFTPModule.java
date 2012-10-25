package stork.module;

import stork.util.*;
import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

public class StorkGridFTPModule extends TransferModule {
  private static final ClassAd info_ad = new ClassAd();

  private static final String pstr = "gsiftp,gridftp,gftp,ftp";
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

  // Ad sink to allow for ads from multiple sources. This lets the
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

  // Transfer watcher
  // ----------------
  // Stupid workaround to the fact that jglobus doesn't support
  // watching third party transfers.
  private static class TransferWatcher extends Thread {
    volatile boolean done = false, reallydone = false;
    ClassAd ad = new ClassAd();
    volatile long xferred = 0, total = 0;
    GridFTPTransfer tf;

    public TransferWatcher(GridFTPTransfer tf) {
      this.tf = tf;

      // Get the file size from the source.
      try {
        if (tf.sc != null)
          total = tf.sc.size(tf.su.getPath());
        else
          total = -1;  // TODO: Use File
      } catch (Exception e) {
        message("couldn't get size of source file");
        total = -1;
      }
    }

    private void message(String m) {
      ad.insert("message", m);
      tf.sink.mergeAd(ad);
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

    private void updateAd(long xferred) {
      if (xferred >= 0) {
        ad.insert("bytes_xferred", Long.toString(xferred));
        if (total > 0)
          ad.insert("progress", 
              String.format("%.2f%%", 100.0*xferred/total));
      } tf.sink.mergeAd(ad);
    }

    public void run() {
      if (total >= 0)
        ad.insert("bytes_total", Long.toString(total));

      if (tf.dc != null) while (!done()) try {
        long x = tf.dc.size(tf.du.getPath());
        if (x != xferred)
          updateAd(xferred = x);  // Only update if there's a change.
        sleep(500);  // Wait half a second before checking again.
      } catch (Exception e) {
        message("couldn't get size of dest file");
      } else while (!done()) {
        // Get file size
      }
    }
  };

  // Transfer class
  // --------------
  static class GridFTPTransfer implements StorkTransfer {
    Thread thread = null;
    ClassAd job;
    GSSCredential cred = null;
    RetrieveOptions ro = new RetrieveOptions();

    FTPClient sc = null, dc = null;
    URI su = null, du = null;
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
      GridFTPClient gcli = null;

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
        cli = gcli = new GridFTPClient(host, (port > 0) ? port : 2811);
      else if (p.equals("ftp"))
        cli = new FTPClient(host, (port > 0) ? port : 21);
      else if (p.equals("file"))
        return null;
      else throw new Exception("unsupported protocol: "+p);

      // If we're using GridFTP and got a cred, try to auth with that.
      if (gcli != null && cred != null)
        gcli.authenticate(cred, user);

      // Otherwise, try username/password if given.
      else if (user != null)
        cli.authorize(user, pass);

      cli.setType(Session.MODE_BLOCK);

      return cli;
    } 

    // Build a transfer list recursively given a source URL. The strings
    // in the returned array should be meaningful relative to u.
    private List<String> xfer_list(URI u, String p) throws Exception {
      String path = u.getPath()+p;
      boolean directory = u.getPath().endsWith("/"),
              wildcard  = u.getPath().indexOf('*') >= 0,
              recursive = directory || wildcard;
      ArrayList<String> list = new ArrayList<String>(20);

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

      if (me == null)
        return list;

      System.out.println(me);

      if (me.get("type").equals(me.TYPE_FILE)) {
        list.add(p);
        return list;
      }

      // Ignore weird file types.
      if (!me.get("type").equals(me.TYPE_DIR))
        return list;

      // TODO: This...
      if (wildcard) {
        return list;
      }

      // It's a directory, we have to recurse.
      if (directory) {
        sc.setPassiveMode(true);
        Object[] mes = sc.mlsd(u.getPath()).toArray();
        list.add(p);  // Returned array will contain the directory.

        for (Object o : mes) {
          if (!(o instanceof MlsxEntry))
            continue;

          MlsxEntry m = (MlsxEntry) o;
          String name = m.getFileName();

          if (m.get("type").equals(m.TYPE_FILE))
            list.add(p+name);
          else if (!me.get("type").equals(me.TYPE_DIR))
            continue;
          else if (name.equals(".") || name.equals(".."))
            continue;
          else
            list.addAll(xfer_list(u, p+name+"/"));
        }
      }

      return list;
    }

    public void run() {
      String in = null;

      // Make sure we have a src and dest url.
      try {
        su = new URI(job.get(in = "src_url"));
        du = new URI(job.get(in = "dest_url"));
      } catch (Exception e) {
        set(255, "couldn't parse "+in+": "+e.getMessage());
        return;
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
        set(255, "error loading x509 proxy: "+e.getMessage());
        return;
      }

      // Attempt to connect to hosts.
      // TODO: Differentiate between temporary errors and fatal errors.
      try {
        in = "src";  sc = connect(su, cred);
        in = "dest"; dc = connect(du, cred);
      } catch (Exception e) {
        set(255, "couldn't connect to "+in+" server: "+e);
        return;
      }

      // If it's file-to-file, don't accept the job.
      if (sc == null || dc == null) {
        //set(255, "no FTP or GridFTP servers specified");
        set(255, "currently only 3rd party transfers supported");
        return;
      }

      // Start a timer to watch the file size at the destination.
      // Stupid workaround because jglobus can't watch third party transfers.
      TransferWatcher tw = new TransferWatcher(this);

      // TODO: If doing 3rd party, check if two servers can talk.

      // Generate a transfer list from the source URL.
      String[] src_list = {}, dst_list = {}, dir_list = {};

      try {
        ArrayList<String> sl = new ArrayList<String>();
        ArrayList<String> dl = new ArrayList<String>();
        ArrayList<String> fl = new ArrayList<String>();

        for (String s : xfer_list(su, "")) {
          String dp = du.getPath()+s;
          if (dp.endsWith("/")) {
            fl.add(dp);
            System.out.println("DIRECTORY: "+dp);
          } else {
            sl.add(su.getPath()+s);
            dl.add(dp);
            System.out.println("FILE: "+dp);
          }
        }

        src_list = sl.toArray(src_list);
        dst_list = dl.toArray(dst_list);
        dir_list = fl.toArray(dir_list);
      } catch (Exception e) {
        set(255, "error generating transfer list: "+e);
        e.printStackTrace();
        return;
      }

      // Set options according to job ad
      if (job.has("parallelism")) {
        int def_p = job.getInt("parallelism", ro.getStartingParallelism()),
            min_p = job.getInt("min_parallelism", def_p),
            max_p = job.getInt("max_parallelism", def_p);

        if (min_p > def_p || def_p > max_p) {
          set(255, "inconsistency in parallelism constraints");
          return;
        }

        ro.setStartingParallelism(def_p);
        ro.setMinParallelism(min_p);
        ro.setMaxParallelism(max_p);
      } try {
        if (sc != null) sc.setOptions(ro);
        if (dc != null) dc.setOptions(ro);
      } catch (Exception e) {
        set(255, "error setting parallelism level");
        return;
      }

      // Put servers into state to prepare for transfer.
      try {
        sc.setType(Session.TYPE_IMAGE);
        dc.setType(Session.TYPE_IMAGE);
      } catch (Exception e) {
        set(255, "couldn't prepare servers for transfer: "+e.getMessage());
        e.printStackTrace();
        return;
      }

      // Perform transfer on everything in the list. Output ads.
      try {
        if (dc instanceof GridFTPClient && sc instanceof GridFTPClient) {
          GridFTPClient gsc = (GridFTPClient) sc, gdc = (GridFTPClient) dc;

          // Make dirs first...
          //for (String s : dir_list)
            //if (!dc.exists(s)) dc.makeDir(s);

          // Set to extended block mode.
          gsc.setMode(GridFTPSession.MODE_EBLOCK);
          gdc.setMode(GridFTPSession.MODE_EBLOCK);

          // Create completion listener
          final int total = dst_list.length;
          MultipleTransferCompleteListener mtcl;
          mtcl = new MultipleTransferCompleteListener() {
            int i = 0;
            public void transferComplete(MultipleTransferComplete mtc) {
              ClassAd ad = new ClassAd();
              ad.insert("file_progress", (++i)+"/"+total);
              sink.mergeAd(ad);
            }
          };

          // Bulk transfer
          gsc.setActive(gdc.setPassive());
          gsc.extendedMultipleTransfer(src_list, gdc, dst_list, null, mtcl);
        }
      } catch (Exception e) {
        set(255, "couldn't transfer: "+e.getMessage());
        e.printStackTrace();
        return;
      }

      set(0, null);
      tw.done(true);
      sink.close();
      close();
    }

    private void close() {
      try {
        if (sc != null) sc.close();
        if (dc != null) dc.close();
      } catch (Exception e) { }
    }

    public void start() {
      thread = new Thread(this);
      thread.start();
    }

    public void stop() {
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
  public static void main(String args[]) {
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
        System.out.println("Invalid arguments...");
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
}
