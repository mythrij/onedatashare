package stork.module.sftp;

import stork.ad.*;
import stork.util.*;
import stork.module.*;
import stork.scheduler.*;
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

    info_ad = new ModuleInfoAd(ad);
  }

  public ModuleInfoAd infoAd() {
    return info_ad;
  }

  public Ad validateAd(SubmitAd ad) {
    return ad;
  }

  // Create a new connection to an FTP server.
  public StorkSession session(EndPoint e) {
    return null;
  }
}
