package stork.module;

import stork.ad.*;
import stork.feather.*;
import stork.util.*;
import stork.scheduler.*;

import java.util.*;
import java.io.*;
import java.net.URI;

// A transfer module which executes as an independent process.

public class ExternalModule extends TransferModule {
  private transient File exe;

  // Inner class representing a transfer.
  class ExternalTransfer extends Thread {
    Thread thread = null;
    Ad job;
    long bytes_xferred = 0, bytes_total = 0;
    int rv = -1;
    Process proc = null;
    InputStream proc_is = null;

    ExternalTransfer(Ad job) {
      this.job = job;
    }

    public void run() {
      // Get arguments from Ad
      String cmd, args = job.get("arguments", "");
      String src = job.get("src"), dest = job.get("dest");

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

    public void terminate() {
      try {
        proc.destroy();
        proc = null;
      } catch (Exception e) {
        // Do nothing...
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
    return execute(exe, args);
  } private static Process execute(File exe, String args) {
    try {
      return Runtime.getRuntime().exec(exe.getPath()+" "+args);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Not supported yet...
  public Ad list(String url, Ad opts) {
    throw new UnsupportedOperationException();
  }

  public ExternalModule(File exe) {
    super(readInfoAd(exe));
    this.exe = exe;
  }

  // Get an info ad by executing this file with the -i option.
  private static Ad readInfoAd(File exe) {
    if (!exe.isAbsolute())
      exe = exe.getAbsoluteFile();

    // Check that it's a valid executable.
    if (!exe.canExecute() || !exe.isFile() || exe.isHidden())
      throw new RuntimeException("file is not a valid executable");

    // Read info ad from executable.
    Process p = execute(exe, "-i");

    // Make sure it worked first!
    if (p == null)
      throw new RuntimeException("couldn't execute transfer module");

    // Attempt to read ad.
    try {
      return Ad.parse(p.getInputStream());
    } catch (Exception e) {
      throw new RuntimeException("couldn't parse module info ad", e);
    }
  }

  // Just to satisfy the interface for now.
  public Session session(Endpoint e) {
    return null;
  }

  public void closeImpl() { }
}
