package stork.module;

import stork.ad.*;
import stork.util.*;
import stork.scheduler.*;

import java.util.*;
import java.io.*;
import java.net.URI;

// A transfer module which executes as an independent process.

public class ExternalModule extends TransferModule {
  private ModuleInfoAd info_ad = null;
  private File exe;

  // Inner class representing a transfer.
  class ExternalTransfer extends StorkTransfer {
    Thread thread = null;
    SubmitAd job;
    long bytes_xferred = 0, bytes_total = 0;
    int rv = -1;
    Process proc = null;
    InputStream proc_is = null;

    ExternalTransfer(SubmitAd job) {
      super(null, job);
      this.job = job;
    }

    public void run() {
      // Get arguments from Ad
      String cmd, args = job.get("arguments", "");
      String src = job.src.toString(), dest = job.dest.toString();

      cmd = String.format("%s %s %s", args, src, dest).trim();

      // Don't run twice!
      if (proc != null) return;

      try {
        proc = execute(cmd);
        proc_is = proc.getInputStream();
      } catch (Exception e) {
        rv = 255; return;
      }
    }

    public void start() {
      run();
    }

    public void stop() {
      try {
        proc.destroy();
        proc = null;
      } catch (Exception e) {
        System.out.println("Warning: error stopping "+proc);
      } rv = 255;
    }

    public int waitFor() {
      try {
        int r = proc.waitFor();
        if (rv < 0) rv = r;  // Only set if rv = -1.
      } catch (Exception e) {
        rv = (rv >= 0) ? rv : 255;
      } return rv;
    }

    // Get an update ad from the job.
    public Ad getAd() {
      if (proc != null && proc_is != null) try {
        return Ad.parse(proc_is);
      } catch (Exception e) {
        Ad ad = new Ad("error", e.getMessage());
        try {
          proc_is.close();
        } catch (Exception ex) { }
        proc_is = null;
        return null;
      } else {
        return null;
      }
    }
  }

  // Execute a command and return the process handler for it.
  // TODO: Security considerations.
  private Process execute(String args) {
    try {
      return Runtime.getRuntime().exec(exe.getPath()+" "+args);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  // Not supported yet...
  public Ad list(String url, Ad opts) {
    throw new UnsupportedOperationException();
  }

  public ExternalModule(File exe) {
    if (!exe.isAbsolute())
      exe = exe.getAbsoluteFile();
    this.exe = exe;

    // Check that it's a valid executable.
    if (!exe.canExecute() || !exe.isFile() || exe.isHidden())
      throw new FatalEx("file is not a valid executable");

    // Read info ad from executable.
    Process p = execute("-i");

    // Make sure it worked first!
    if (p == null)
      throw new FatalEx("couldn't execute transfer module");

    // Attempt to read ad.
    try {
      Ad ad = Ad.parse(p.getInputStream());
      info_ad = new ModuleInfoAd(ad);
    } catch (Exception e) {
      throw new FatalEx("couldn't parse module info ad");
    }
  }

  // Just to satisfy the interface for now.
  public StorkSession session(EndPoint e) {
    return null;
  }

  public void closeImpl() { }

  // Return cached info ad or read new one if necessary.
  public ModuleInfoAd infoAd() {
    return info_ad;
  }

  // Validate a job ad and a new ad with only the fields expected
  // by this transfer module.
  public Ad validateAd(SubmitAd ad) throws Exception {
    Ad ad1 = ad.filter("arguments");
    Ad ad2 = ad.filter(info_ad.opt_params);
    Ad ad3 = ad.filter(info_ad.req_params);
    ad3.require(info_ad.req_params);
    return ad1.merge(ad2, ad3);
  }

  public StorkTransfer transfer(SubmitAd ad) {
    return new ExternalTransfer(ad);
  }
}
