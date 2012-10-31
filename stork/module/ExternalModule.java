package stork.module;

import stork.util.*;
import java.util.*;
import java.io.*;
import java.net.URI;

// A transfer module which executes as an independent process.

public class ExternalModule extends TransferModule {
  private ClassAd info_ad = null;
  private File exe;
  private String[] protocols = null;
  private String name = null;
  private String version = null;

  // Inner class representing a transfer.
  class ExternalTransfer implements StorkTransfer {
    Thread thread = null;
    ClassAd job;
    long bytes_xferred = 0, bytes_total = 0;
    int rv = -1;
    Process proc = null;

    ExternalTransfer(ClassAd job) {
      this.job = job;
    }

    // TODO: Rumor has it Java doesn't guarantee processes execute in
    // parallel. Maybe have some additional checks?
    public void run() {
      // Get arguments from ClassAd
      String args = job.get("arguments");
      URI src, dest;

      // Don't run twice!
      if (proc != null) return;

      // Parse src and dest URL so they're normalized.
      try {
        src = new URI(job.get("src_url"));
        dest = new URI(job.get("dest_url"));
      } catch (Exception e) {
        rv = 255; return;
      }

      try {
        proc = (args != null) ? execute(args+" "+src+" "+dest) :
                                execute(src+" "+dest);
      } catch (Exception e) {
        rv = 255; return;
      }
    }

    public void start() { run(); }

    public void stop() {
      try {
        proc.destroy();
        rv = proc.waitFor();
      } catch (Exception e) {
        System.out.println("GOT AN ERROR KILLING PROCESS!");
      }
    }

    public int waitFor() {
      try {
        return rv = proc.waitFor();
      } catch (Exception e) {
        return (rv >= 0) ? rv : 255;
      }
    }

    // Get an update ad from the job.
    public ClassAd getAd() {
      if (proc != null)
        return ClassAd.parse(proc.getInputStream());
      else
        return null;
    }
  }

  // Only public way to create a new ExternalModule. Handles verification.
  public static ExternalModule create(File exe) {
    if (!exe.isAbsolute())
      exe = exe.getAbsoluteFile();

    // Check that it's a valid executable.
    if (!exe.canExecute() || !exe.isFile() || exe.isHidden())
      return null;

    ExternalModule tm = new ExternalModule(exe);

    // Run it and read the info ad.
    if (tm.info_ad() == null)
      return null;

    return tm;
  }

  // Execute a command and return the process handler for it.
  // TODO: Security considerations.
  private Process execute(String args) {
    try {
      return Runtime.getRuntime().exec(exe.getPath()+" "+args);
    } catch (Exception e) {
      return null;
    }
  }

  // Assume the executable is valid, since we'll validate it
  // before creating this.
  private ExternalModule(File exe) {
    this.exe = exe;
  }

  // Return cached info ad or read new one if necessary.
  public ClassAd info_ad() {
    if (info_ad != null)
      return info_ad;

    Process p = execute("-i");
    ClassAd ad;

    // Make sure it worked first!
    if (p == null) return null;

    // Attempt to read ad.
    ad = ClassAd.parse(p.getInputStream());

    if (ad == null || ad == ClassAd.EOF)
      return null;

    // Verify that it's an info ad.
    if (!ad.has("protocols") || !ad.has("name"))
      return null;

    // Store transfer module information
    protocols = splitProtocols(ad.get("protocols"));
    name = ad.get("name");
    version = ad.get("version");

    return info_ad = ad;
  }

  public String[] protocols() { return protocols; }
  public String name() { return name; }
  public String version() { return version; }

  public StorkTransfer transfer(ClassAd ad) {
    return new ExternalTransfer(ad);
  }
}
