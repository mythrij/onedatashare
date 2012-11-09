package stork.module;

import stork.util.*;
import java.util.*;
import java.io.*;
import java.net.URI;

// A transfer module which executes as an independent process.

public class ExternalModule extends TransferModule {
  private ModuleInfoAd info_ad = null;
  private File exe;

  // Inner class representing a transfer.
  class ExternalTransfer implements StorkTransfer {
    Thread thread = null;
    SubmitAd job;
    long bytes_xferred = 0, bytes_total = 0;
    int rv = -1;
    Process proc = null;
    InputStream proc_is = null;

    ExternalTransfer(SubmitAd job) {
      this.job = job;
    }

    // TODO: Rumor has it Java doesn't guarantee processes execute in
    // parallel. Maybe have some additional checks?
    public void run() {
      // Get arguments from ClassAd
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
    public ClassAd getAd() {
      if (proc != null && proc_is != null) try {
        ClassAd ad = ClassAd.parse(proc_is);

        if (ad.error()) {
          proc_is.close();
          proc_is = null;
          return null;
        }
        
        return ad;
      } catch (Exception e) {
        ClassAd ad = new ResponseAd("error", e.getMessage());
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

  public ExternalModule(File exe) throws Exception {
    if (!exe.isAbsolute())
      exe = exe.getAbsoluteFile();
    this.exe = exe;

    // Check that it's a valid executable.
    if (!exe.canExecute() || !exe.isFile() || exe.isHidden())
      throw new Exception("file is not a valid executable");

    // Read info ad from executable.
    Process p = execute("-i");

    // Make sure it worked first!
    if (p == null)
      throw new Exception("couldn't execute transfer module");

    // Attempt to read ad.
    try {
      ClassAd ad = ClassAd.parse(p.getInputStream());
      info_ad = new ModuleInfoAd(ad);
    } catch (Exception e) {
      throw new Exception("couldn't parse module info ad");
    }
  }

  // Return cached info ad or read new one if necessary.
  public ModuleInfoAd infoAd() {
    return info_ad;
  }

  // Validate a job ad and a new ad with only the fields expected
  // by this transfer module.
  public ClassAd validateAd(SubmitAd ad) throws Exception {
    ClassAd ad1 = ad.filter("arguments");
    ClassAd ad2 = ad.filter(info_ad.opt_params);
    ClassAd ad3 = ad.filter(info_ad.req_params);
    ad3.require(info_ad.req_params);
    return ad1.merge(ad2, ad3);
  }

  public StorkTransfer transfer(SubmitAd ad) {
    return new ExternalTransfer(ad);
  }
}
