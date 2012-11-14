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
    ad.insert("opt_params",
              "parallelism,x509_proxy");

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
      putAd((ad != null) ? ad.merge(a) : a);
    }
    
    // Block until an ad has come in, then clear the ad.
    public synchronized ClassAd getAd() {
      if (more) try {
        wait();
        if (closed) more = false;
        return ad;
      } catch (Exception e) { }
      return null;
    }
  }

  // A tree representing a file hierarchy to be transferred.
  static class XferTree implements Iterable<XferTree> {
    final List<XferTree> subtrees;
    String name;
    boolean done = false;
    long off = 0;
    private long size;

    // Constructor for directory.
    XferTree(String path) {
      subtrees = new LinkedList<XferTree>();
      name = path;
      size = 0;
    }

    // Constructor for file.
    XferTree(String name, long size) {
      subtrees = null;
      this.name = name;
      this.size = size;
    }

    boolean isDirectory() {
      return subtrees != null;
    }

    boolean isFile() {
      return subtrees == null;
    }

    void add(XferTree tree) {
      if (isFile()) return;
      subtrees.add(tree);
      size += tree.size;
    }

    void add(String name, long size) {
      add(new XferTree(name, size));
    }

    void add(String name) {
      add(new XferTree(name));
    }

    long remaining() {
      return (off < size) ? size-off : 0;
    }

    XferTree subtree(long size) {
      XferTree nt = new XferTree(name);
      long total = 0;

      for (XferTree x : this) {
        nt.add(x);
        if (x.isFile()) total += x.remaining();
        if (total >= size) break;
      } return nt;
    }

    /*
    XferTree[] split(int parts) {
      if (parts <= 1)
        return new XferTree[] { this };

      XferTree[] a = new XferTree[parts];
    */

      

    // Iterator for this thing. Returns self first, then returns
    // children, iterating into directories recursively.
    public Iterator<XferTree> iterator() {
      return new Iterator<XferTree>() {
        private XferTree me = XferTree.this;
        private Iterator<XferTree> di  =
          isDirectory() ? subtrees.iterator() : null;
        private Iterator<XferTree> xi = null;
        public boolean hasNext() {
          return me != null || xi != null || di != null;
        } public XferTree next() {
          XferTree r =
            (me != null) ? me :
            (xi != null) ? xi.next() :
            (di != null) ? (xi = di.next().iterator()).next() : null;
          me = null;
          if (xi != null && !xi.hasNext()) xi = null;
          if (di != null && !di.hasNext()) di = null;
          return r;
        } public void remove() { /* maybe one day */ }
      };
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

    // Get the list from the sink as an XferTree.
    public XferTree getList(String path) {
      XferTree xt = new XferTree(path);
      String line;

      // Read lines from the buffer list.
      while ((line = readLine()) != null) {
        try {
          MlsxEntry m = new MlsxEntry(line);

          String name = m.getFileName();
          String type = m.get("type");
          String size = m.get("size");

          if (type.equals(m.TYPE_FILE))
            xt.add(path+name, Long.parseLong(size));
          else if (!name.equals(".") && !name.equals(".."))
            xt.add(path+name);
        } catch (Exception e) {
          e.printStackTrace();
          continue;  // Weird data I guess!
        }
      } return xt;
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
      p_base = p_range.min()-1;
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

        pd *= 2;
        para = p_base+pd;

        if (para > p_range.max())
          para = p_range.max();

        /*  Don't do this for now...
        if (b.tp < last_tp)
          done_sampling = true;

        last_tp = b.tp;
        */
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

    private int parallelism = 1, pipelining = 50, concurrency = 1;
    private int max_parallelism = 1, min_parallelism = 1;

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

    public void setParallelism(int p) {
      p = (p < 1) ? 1 : p;
      parallelism = p;
      try {
        controlChannel.execute(
          new Command("OPTS", "RETR Parallelism="+p+","+p+","+p+";"));
      } catch (Exception e) {
        System.out.println("Wasn't able to set parallelism...");
      }
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
    public void watchTransfer(DataSink sink, MarkerListener ml)
    throws Exception {
      TransferState ts = new TransferState();
      TransferMonitor tm;
      if (sink != null) localServer.store(sink);

      tm = new TransferMonitor(controlChannel, ts, ml, 50000, 2000, 1);
      tm.setOther(tm);
      tm.run();

      if (ts.hasError())
        throw ts.getError();
    }

    // Read and handle the response of a pipelined PASV.
    public void getPasvReply() throws Exception {
      Reply r = controlChannel.read();
      String s = r.getMessage().split("[()]")[1];
      session.serverMode = Session.SERVER_PASSIVE;
      session.serverAddress = new HostPort(s);
      setLocalActive();
    }

    // Recursively list directories.
    public XferTree mlsr(String path) throws Exception {
      FTPControlChannel cc = controlChannel;
      XferTree list = new XferTree(path);
      final String MLSR = "MLSR", MLSD = "MLSD";
      String ls_cmd = isFeatureSupported("MLSR") ? MLSR : MLSD;

      if (!path.endsWith("/")) path += "/";

      LinkedList<String> dirs = new LinkedList<String>();
      dirs.add(path);

      try {
        cc.execute(new Command("OPTS MLST type;size;"));
      } catch (Exception e) { /* who cares */ }

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
          cc.write(Command.PASV);
          cc.write(new Command(ls_cmd, p));
        }

        // Read the pipelined responses like a champ.
        System.out.println("reading piped responses");
        for (String p : working) {
          ListSink sink = new ListSink();

          // Interpret the pipelined PASV command.
          try {
            getPasvReply();
          } catch (Exception e) {
            throw new Exception("couldn't set passive mode for MLSR: "+e);
          }

          watchTransfer(sink, null);

          XferTree xt = sink.getList(p);

          // If we did mlsr, return the list. Else, repeat.
          if (ls_cmd == MLSR)
            return xt;
          
          for (XferTree e : xt)
            if (e.isDirectory()) subdirs.add(p+"/"+e.name);
          list.add(xt);
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
        close();

        if (dest != null && dest != this) {
          dest.controlChannel.write(Command.ABOR); 
          dest.close();
        }
      } catch (Exception e) {
        // We don't care if aborting failed!
      }
      aborted = true;
    }

    // Transfer a whole list of files.
    // TODO: Refactor both transfer()s into one method.
    void transfer(XferTree xl, ProgressListener pl) throws Exception {
      if (aborted)
        throw abort_exception;

      if (dest == null)
        throw new Exception("call setDest first");

      checkGridFTPSupport();
      dest.checkGridFTPSupport();

      gSession.matches(dest.gSession);

      // First pipeline all the commands.
      for (XferTree x : xl) {
        if (aborted)
          throw abort_exception;

        // We have to make a directory.
        if (x.size == -1) {
          Command cmd = new Command("MKD", x.name);
          dest.controlChannel.write(cmd);
        }

        // We have to transfer a file.
        else {
          Command dcmd = new Command("STOR", x.name);
          dest.controlChannel.write(dcmd);

          Command scmd = new Command("RETR", x.name);
          controlChannel.write(scmd);
        }
      }

      // Then watch the file transfers.
      for (XferTree x : xl) {
        if (aborted)
          throw abort_exception;
        if (x.size == -1)  // Read and ignore mkdir results.
          dest.controlChannel.read();
        else
          dest.watchTransfer(null, pl);
        //xl.done(x);
        if (pl != null)
          pl.fileComplete(x);
      } pl.transferComplete();
    }

    // Transfer a single file.
    void transfer(SubmitAd ad, boolean opt, ProgressListener pl)
    throws Exception {
      Range p_range;
      String sp = ad.src.getPath();
      String dp = ad.dest.getPath();

      if (aborted)
        throw abort_exception;

      if (dest == null)
        throw new Exception("call setDest first");

      if (sp.startsWith("/~")) sp = sp.substring(1);
      if (dp.startsWith("/~")) dp = dp.substring(1);

      checkGridFTPSupport();
      dest.checkGridFTPSupport();

      gSession.matches(dest.gSession);
      setMode(GridFTPSession.MODE_EBLOCK);
      dest.setMode(GridFTPSession.MODE_EBLOCK);

      Optimizer optimizer;
      long size = size(sp);

      // Check min and max parallelism.
      if (ad.has("parallelism"))
        p_range = Range.parseRange(ad.get("parallelism"));
      else
        p_range = new Range(1, 64);

      if (p_range == null || !p_range.isContiguous())
        throw new Exception("parallelism must be a number or range");
      if (p_range.min() <= 0)
        throw new Exception("parallelism must be greater than zero");

      // Initialize optimizer.
      if (opt)
        optimizer = new Full2ndOptimizer(size, 0, p_range);
      else
        optimizer = new NullOptimizer(size, 0);

      optimizer.para = p_range.min();

      // Begin transferring according to optimizer.
      while (!optimizer.done()) {
        Block b = optimizer.getBlock();
        TransferProgress prog = new TransferProgress();

        // Set parameters
        if (b.para > 0) {
          setParallelism(b.para);
          if (pl != null)
            pl.sink.mergeAd(new ClassAd().insert("parallelism", b.para));
        }

        prog.setBytes(b.len);
        prog.transferStarted();

        extendedTransfer(sp, b.off, b.len, dest, dp, b.off, pl);

        prog.bytesDone(b.len);
        prog.transferEnded();
        b.tp = prog.throughputValue(true)/1000/1000;
        optimizer.report(b);
      }
        
      // Let the progress listener hear the good news.
      if (pl != null) {
        pl.fileComplete(0);
        pl.transferComplete();
      }
    }
  }

  private static class ProgressListener implements MarkerListener {
    volatile boolean done = false, reallydone = false;
    ClassAd ad = new ClassAd();
    int num_done = 0, num_total = 0;
    long last_bytes = 0;
    XferTree list = null;
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
    public ProgressListener(AdSink sink, XferTree list) {
      this.sink = sink;
      this.list = list;
      prog = new TransferProgress();
      prog.setBytes(list.size);
      //prog.setFiles(list.count);
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
    public void fileComplete(XferTree e) {
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
  }

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
      XferTree xfer_list = null;

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
        sc.setDest(dc);
        sc.transfer(job, true, pl);
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

  public static void main3(String args[]) {
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
      XferTree xl = sc.mlsr(uri.getPath());

      for (XferTree e : xl)
        System.out.println(e.name);
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
