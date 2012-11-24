package stork.module;

import stork.util.*;
import stork.stat.InvQuadRegression;
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
    ad.insert("opt_params", "parallelism,x509_proxy");

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
    private LinkedList<Buffer> buf_list;
    private Buffer cur_buf = null;
    private BufferedReader br;
    private int off = 0;

    public ListSink() {
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
    public XferList getList(String path) {
      XferList xl = new XferList(path, "");
      String line;

      // Read lines from the buffer list.
      while ((line = readLine()) != null) {
        try {
          MlsxEntry m = new MlsxEntry(line);

          String name = m.getFileName();
          String type = m.get("type");
          String size = m.get("size");

          if (type.equals(m.TYPE_FILE))
            xl.add(name, Long.parseLong(size));
          else if (!name.equals(".") && !name.equals(".."))
            xl.add(name);
        } catch (Exception e) {
          e.printStackTrace();
          continue;  // Weird data I guess!
        }
      } return xl;
    }
  }

  // Optimization  TODO: Refactor me!
  // ------------
  // Optimizers work by returning block structures containing portions
  // of transfers in bytes and transfer settings for the block. When
  // the client has transferred the block, it gives the block object with
  // tp field filled out back to the optimizer, which the optimizer
  // should use for statistical analysis to determine block settings.

  // The class all optimizers extend.
  static abstract class Optimizer {
    protected long size, off = 0;
    protected int para = 0, pipe = 0, conc = 0;

    Optimizer(long size, long off) {
      this.size = size;
      this.off = off;
    }

    // Return a block which has this optimizers current values.
    Block defaultBlock() {
      Block b = new Block(off, size-off);
      b.para = para; b.pipe = pipe; b.conc = conc;
      return b;
    }

    abstract Block getBlock();
    abstract void report(Block b);

    boolean done() {
      return off >= size;
    }
  }

  static class Block {
    long off, len;
    int para = 0, pipe = 0, conc = 0;
    double tp = 0;  // Throughput - filled out by caller

    Block(long o, long l) {
      off = o; len = l;
    }

    public String toString() {
      return String.format("<off=%d, len=%d | sc=%d, tp=%.2f>", off, len, para, tp);
    }
  }

  // An "optimizer" that just transfers the whole file in one block
  // with whatever transfer options it's set to use.
  static class NullOptimizer extends Optimizer {
    NullOptimizer(long size, long off) {
      super(size, off);
    }

    Block getBlock() {
      return defaultBlock();
    }

    void report(Block b) {
      off += b.len;
    }
  }

  // Full 2nd order optimizer. Sampling starts at the parallelism
  // minimum, and increases by powers of two (relative to the min).
  // The sampling stops when throughput is found to have decreased
  // sampling stops once the max parallelism is reached, taking a
  // final sample at the maximum. Then an analysis is done, and a
  // function describing the throughput relative to parallelism
  // is produced and used to choose the best parallelism for the
  // transfer.
  static class Full2ndOptimizer extends Optimizer {
    List<Block> samples = new ArrayList<Block>();
    boolean warmed_up = false;
    boolean done_sampling = false, analysis_done = false;
    double last_tp = 0;
    int p_base = 0, pd = 1;
    Range p_range;

    Full2ndOptimizer(long size, long off, Range p_range) {
      super(size, off);
      this.p_range = p_range;
      para = p_range.min();
      p_base = para-1;
    }

    Block getBlock() {
      if (done_sampling) {
        if (!analysis_done) doAnalysis();
        return defaultBlock();
      }

      long sample = (long) ((size >= 5E8) ? 5E7 : size/10.0);

      // Don't transfer more than what's available.
      if (off+sample >= size)
        sample = this.size - this.off;

      // Determine if this is the last sample we want.
      if (para >= p_range.max()) {
        para = p_range.max();
        done_sampling = true;
      }

      // Construct the block to transfer.
      Block b = new Block(off, sample);
      b.para = para;
      return b;
    }

    void report(Block b) {
      if (done()) return;

      off += b.len;

      // If that was a warm-up sample, don't change anything.
      if (!warmed_up) {
        System.out.println("Alright, that was a warm-up...");
        warmed_up = true;
      } else if (!done_sampling) {
        // Keep the sample and calculate next parallelism.
        System.out.println("Block: "+b);
        samples.add(b);

        System.out.println("Added sample: "+b);

        pd *= 2;
        para = p_base+pd;

        if (para > p_range.max()) {
          para = p_range.max();
          done_sampling = true;
        }

        if (b.tp < last_tp)
          done_sampling = true;
        last_tp = b.tp;
      }
    }

    // Get the best parallelism from the samples.
    int bestParallelism() {
      double tp = 0;
      int sc = para;

      for (Block b : samples) if (b.tp > tp) {
        tp = b.tp;
        sc = b.para;
      } return sc;
    }

    // Will return negative if result should be ignored.
    double cal_err(double a, double b, double c) {
      double sqr_sum = 0.0;
      int n = samples.size();
      int df = 1;

      // Sum squares of differences between predicted and actual throughput
      for (Block bl : samples) {
        double thr = cal_thr(a, b, c, bl.para);
        if (thr <= 0)
          df = -1;
        else
          sqr_sum += (bl.tp-thr)*(bl.tp-thr);
      }

      return df * Math.sqrt(sqr_sum/n);
    }

    // Calculate the difference of two terms n2^2/thn2^2 n1^2/thn1^2 given
    // two samples.
    static double cal_dif(Block s1, Block s2) {
      int n1 = s1.para, n2 = s2.para;
      return (n1*n1/s1.tp/s1.tp - n2*n2/s2.tp/s2.tp) / (n1-n2);
    }

    static double cal_a(Block i, Block j, Block k) {
      return (cal_dif(k, i) - cal_dif(j, i)) / (k.para - j.para);
    }

    static double cal_b(Block i, Block j, Block k, double a) {
      return cal_dif(j, i) - (i.para + j.para) * a;
    }

    static double cal_c(Block i, Block j, Block k, double a, double b) {
      int ni = i.para;
      return ni*ni/i.tp/i.tp - ni*ni*a - ni*b;
    }

    // Calculate the throughput of n streams as predicted by our model.
    static double cal_thr(double a, double b, double c, int n) {
      if (a*n*n + b*n + c <= 0) return 0;
      return n / Math.sqrt(a*n*n + b*n + c);
    }

    // Calculate the optimal stream count based on the prediction model.
    static int cal_full_peak2(double a, double b, double c, Range r) {
      int n = r.min();
      double thr = cal_thr(a, b, c, 1);

      for (int i : r) {
        double t = cal_thr(a, b, c, i);
        if (t < thr) return n;
        thr = t; n = i;
      } return r.max();
    }

    // Calculate the optimal stream count based on derivative.
    static int cal_full_peak(double a, double b, double c, Range r) {
      int n = (int) (-2*c/b);
      
      return (n > r.max()) ? r.max() :
             (n < r.min()) ? r.min() : n;
    }

    // Perform the analysis which sets parallelism. Once the
    // parallelism is set here, nothing else should change it.
    private double doAnalysis2() {
      if (samples.size() < 3) {
        System.out.println("Not enough samples!");
        para = bestParallelism();
        return 0;
      }

      int i, j, k, num = samples.size();
      double a = 0, b = 0, c = 0, err = Double.POSITIVE_INFINITY;
      
      // Iterate through the samples, find the "best" three.
      for (i = 0;   i < num-2; i++)
      for (j = i+1; j < num-1; j++)
      for (k = j+1; k < num;   k++) {
        Block bi = samples.get(i);
        Block bj = samples.get(j);
        Block bk = samples.get(k);

        System.out.printf("Samples: %d %d %d\n", bi.para, bj.para, bk.para);
        System.out.printf("         %.2f %.2f %.2f\n", bi.tp, bj.tp, bk.tp);

        double a_ = cal_a(bi, bj, bk),
               b_ = cal_b(bi, bj, bk, a_),
               c_ = cal_c(bi, bj, bk, a_, b_),
               err_ = cal_err(a_, b_, c_);
        System.out.printf("Got: %.2f %.2f %.2f %.2f\n", a_, b_, c_, err_);

        // If new err is better, replace old calculations.
        if (err_ > 0 && err_ < err) {
          err = err_; a = a_; b = b_; c = c_;
        }
      }

      para = cal_full_peak(a, b, c, p_range);
      analysis_done = true;

      System.out.printf("ANALYSIS DONE!! Got: x/sqrt((%f)*x^2+(%f)*x+(%f)) = %d\n", a, b, c, para);
      return err;
    }

    // Perform analysis using inverse quadratic regression.
    private void doAnalysis() {
      if (samples.size() < 3) {
        System.out.println("Not enough samples!");
        para = bestParallelism();
        return;
      }

      // For testing, get old method's error first.
      double err1 = doAnalysis2();

      InvQuadRegression iqr = new InvQuadRegression(samples.size());

      // Add transformed samples to quadratic regression.
      for (Block s : samples) {
        int n = s.para;
        double th = s.tp;
        
        if (th < 0) {
          System.out.println("Skipping sample: "+s);
          continue;
        }

        iqr.add(n, 1/(th*th));
      }

      // Moment of truth...
      double[] a = iqr.calculate();

      if (a == null) {
        System.out.println("Not enough samples!");
        para = bestParallelism();
        return;
      }

      para = cal_full_peak(a[0], a[1], a[2], p_range);
      analysis_done = true;

      System.out.printf("ANALYSIS DONE!! Got: x/sqrt((%f)*x^2+(%f)*x+(%f)) = %d\n", a[0], a[1], a[2], para);

      // Compare errors.
      double err2 = cal_err(a[0], a[1], a[2]);

      System.out.printf("Errors: 3p = %.2f, qr = %.2f\n", err1, err2);
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
    public void watchTransfer(DataSink sink, boolean local) throws Exception {
      TransferState ss = new TransferState(), ds = new TransferState();
      BasicClientControlChannel oc;
      TransferMonitor stm, dtm;
      ProgressListener pl = new ProgressListener(progress);

      if (sink != null)
        localServer.store(sink);

      if (local)
        oc = localServer.getControlChannel();
      else
        oc = dest.controlChannel;

      stm = new TransferMonitor(controlChannel, ss, null, 50000, 2000, 1);
      dtm = new TransferMonitor(oc, ds, pl, 50000, 2000, 1);

      stm.setOther(dtm);
      dtm.setOther(stm);
      stm.run(); dtm.run();

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
      XferList list = new XferList(path, "");

      LinkedList<String> dirs = new LinkedList<String>();
      dirs.add(path);

      try {
        cc.execute(new Command("OPTS MLST type;size;"));
      } catch (Exception e) { /* who cares */ }

      // Keep listing and building subdirectory lists.
      // TODO: Replace with pipelining structure.
      while (!dirs.isEmpty()) {
        LinkedList<String> subdirs = new LinkedList<String>();
        LinkedList<String> working = new LinkedList<String>();

        System.out.println("building working list");
        while (working.size() < pipelining && !dirs.isEmpty())
          working.add(dirs.pop());

        // Pipeline commands like a champ.
        System.out.println("piping commands");
        for (String p : working) {
          System.out.println(cmd+" "+p);
          swrite(Command.PASV);
          swrite(cmd, p);
        }

        // Read the pipelined responses like a champ.
        System.out.println("reading piped responses");
        for (String p : working) {
          ListSink sink = new ListSink();

          System.out.println(cmd+" "+p);

          // Interpret the pipelined PASV command.
          try {
            getPasvReply();
          } catch (Exception e) {
            throw new Exception("couldn't set passive mode for MLSR: "+e);
          }

          // Try to get the listing, ignoring errors.
          try {
            watchTransfer(sink, true);
          } catch (Exception e) {
            continue;
          }

          XferList xl = sink.getList(p);

          // If we did mlsr, return the list.
          if (cmd == MLSR)
            return xl;
          
          // Otherwise, add subdirs and repeat.
          for (XferList.Entry e : xl)
            if (e.dir) subdirs.add(e.path());
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

      // If dest path is a directory, append file name.
      if (dp.endsWith("/"))
        dp += StorkUtil.basename(sp);

      System.out.println("Transferring: "+sp+" -> "+dp);

      // See if we're doing a directory transfer and need to build
      // a directory list.
      if (sp.endsWith("/")) try {
        xl = mlsr(sp);
        xl.dp = dp;
      } catch (Exception e) {
        throw new Exception("couldn't get listing for: "+sp);
      } else try {  // Otherwise it's just one file.
        System.out.println("size = "+size(sp));
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
        optimizer = new Full2ndOptimizer(xl.size(), 0, para_range);
        sink.mergeAd(new ClassAd().insert("optimizer", "Full2ndOptimizer"));
      } else {
        System.out.println("File size < 1M, not optimizing...");
        optimizer = new NullOptimizer(xl.size(), 0);
        sink.mergeAd(new ClassAd().insert("optimizer", "none"));
      }

      // mkdir the destination path for this list if it's a directory.
      if (xl.root.dir) {
        sendPipedXfer(xl.root);
        recvPipedXferResp(xl.root);
      }

      // Connect source and destination server.
      HostPort hp = dest.setPassive();
      setActive(hp);

      // Let the progress monitor know we're starting.
      progress.transferStarted(xl.size(), xl.count());
      
      // Begin transferring according to optimizer.
      while (!optimizer.done()) {
        Block b = optimizer.getBlock();
        TransferProgress prog = new TransferProgress();

        // Set parameters
        if (b.para > 0) {
          setParallelism(b.para);
          if (sink != null)
            sink.mergeAd(new ClassAd().insert("parallelism", b.para));
        }

        XferList xs = xl.split(b.len);

        prog.transferStarted(xs.size(), xs.count());
        transferList(xs);
        prog.transferEnded(true);

        b.tp = prog.throughputValue(true)/1000/1000;
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

        // Pipeline p commands at a time, unless pipelining is zero,
        // in which case we have infinite pipelining.
        while (pipelining < 1 || p-- > 0) {
          XferList.Entry x = xl.pop();

          if (x == null)
            break;

          sendPipedXfer(x);
          wl.add(x);
        }

        // Read responses to piped commands.
        for (XferList.Entry e : wl) {
          recvPipedXferResp(e);
          progress.done(0, 1);
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
        if (sc != null) sc.close();
        if (dc != null) dc.close();
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
}
