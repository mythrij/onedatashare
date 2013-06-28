package stork.module;

import stork.ad.*;
import stork.util.*;
import java.net.URI;

public class SFTPModule extends TransferModule {
  static ModuleInfoAd info_ad;

  static {
    Ad ad = new Ad();
    ad.put("name", "Stork SFTP/SCP Module");
    ad.put("version", "0.1");
    ad.put("author", "Brandon Ross");
    ad.put("email", "bwross@buffalo.edu");
    ad.put("description",
              "A module for SFTP/SCP transfers.");
    ad.put("protocols", "scp,sftp");
    ad.put("accepts", "classads");

    try {
      info_ad = new ModuleInfoAd(ad);
    } catch (Exception e) {
      info_ad = null;
      System.out.println("Fatal error parsing SFTPModule info ad");
      e.printStackTrace();
      System.exit(1);
    }
  }

  public ModuleInfoAd infoAd() {
    return info_ad;
  }

  public Ad validateAd(SubmitAd ad) {
    return ad;
  }

  // Create a new connection to an FTP server.
  public StorkSession session(URI url, Ad opts) {
    return null; //new SFTPSession(url, opts);
  }
}
