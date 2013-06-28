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

  public Ad validateAd(SubmitAd ad) {
    return ad.filter(info_ad.opt_params);
  }

  // Create a new connection to an FTP server.
  public StorkSession session(URI url, Ad opts) {
    return new GridFTPSession(url, opts);
  }
}
