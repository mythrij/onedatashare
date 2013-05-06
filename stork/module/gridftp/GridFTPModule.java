package stork.module.gridftp;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import static stork.util.StorkUtil.Static.*;
import stork.stat.*;
import stork.cred.*;

import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;
import org.globus.ftp.vanilla.*;
import org.globus.ftp.extended.*;
import org.ietf.jgss.*;
import org.gridforum.jgss.*;

public class GridFTPModule extends TransferModule {
  static ModuleInfoAd info_ad;

  static {
    Ad ad = new Ad();
    ad.put("name", "Stork GridFTP Module");
    ad.put("version", "0.1");
    ad.put("author", "Brandon Ross");
    ad.put("email", "bwross@buffalo.edu");
    ad.put("description",
              "A rudimentary module for FTP and GridFTP transfers.");
    ad.put("protocols", "gsiftp,gridftp,ftp");
    ad.put("accepts", "classads");
    ad.put("opt_params", "parallelism,x509_proxy,optimizer");

    try {
      info_ad = new ModuleInfoAd(ad);
    } catch (Exception e) {
      info_ad = null;
      System.out.println("Fatal error parsing StorkGridFTPModule info ad");
      e.printStackTrace();
      System.exit(1);
    }
  }

  // Interface methods
  // -----------------
  public ModuleInfoAd infoAd() { return info_ad; }

  public Ad validateAd(SubmitAd ad) throws Exception {
    return ad.filter(info_ad.opt_params);
  }

  // Create a new connection to an FTP server.
  public StorkSession session(URI url, Ad opts) {
    return new GridFTPSession(url, opts);
  }

  // Tester
  // ------
  /*
  public static void main1(String args[]) {
    SubmitAd ad = null;
    StorkGridFTPModule m = new StorkGridFTPModule();
    StorkTransfer tf = null;
    String optimizer = null;

    try {
      switch (args.length) {
        case 0:
          System.out.println("Enter an Ad:");
          ad = new SubmitAd(Ad.parse(System.in));
          break;
        case 1:
          ad = new SubmitAd(Ad.parse(new FileInputStream(args[0])));
          break;
        case 3:
          optimizer = args[2];
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
        ad.put("x509_proxy", sb.toString());
    } catch (Exception e) {
      System.out.println("Couldn't open x509_file...");
    }

    ad.put("optimizer", optimizer);

    try {
      ad.setModule(m);
    } catch (Exception e) {
      System.out.println("Error: "+e);
    }

    System.out.println("Starting...");
    tf = m.transfer(ad);
    tf.start();

    while (true) {
      Ad cad = tf.getAd();

      if (cad != null)
        System.out.println("Got ad: "+cad);
      else break;
    }

    int rv = tf.waitFor();

    System.out.println("Job done with exit status "+rv);
  }

  public static void main(String args[]) {
    try {
      // Parse URL
      URI uri = new URI(args[0]).normalize();

      int depth = 0;
      if (args.length > 1)
        depth = Integer.parseInt(args[1]);

      // Open connection
      System.out.println("Reading credentials...");
      Ad opts = new Ad("cred_file", "/tmp/x509up_u1000")
                  .put("depth", depth);

      System.out.println("Listing...");

      Watch watch = new Watch(true);
      Ad xl = new StorkGridFTPModule().list(uri, opts);

      System.out.println("Done listing!");
      System.out.println(xl);
      System.out.println("Duration: "+watch);
    } catch (Exception e) {
      System.out.println("Error: "+e);
      e.printStackTrace();
    }
  }

  public static void main3(String[] args) {
    final TransferProgress tp = new TransferProgress();

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
  */
}
