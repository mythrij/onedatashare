package stork.ad;

import stork.util.*;
import stork.module.*;
import java.net.*;

// A special Ad which describes a job. Will automatically apply
// filters based on protocol. Source and destination URLs are available
// through src and dest fields, and are guaranteed to have a protocol,
// which can be accessed through src_proto and dest_proto.
//
// Once a SubmitAd has been validated 

public class SubmitAd extends Ad {
  public final URI src, dest;
  public final String src_proto, dest_proto;
  public TransferModule tm = null;
  
  static final String[] params =
    { "src", "dest", "module", "dap_type", "max_attempts" };

  // Make a URI from string and store to ad. which = src or dest
  private URI makeURI(String which, String uri) throws Exception {
    try {
      URI u = new URI(uri).normalize();

      // Make sure a protocol was specified!
      if (u.getScheme() == null || u.getScheme().isEmpty())
        throw new Exception("no protocol specified");

      put(which, u.toString());
      return u;
    } catch (Exception e) {
      throw new Exception("error parsing "+which+": "+e.getMessage());
    }
  } private URI makeURI(String which) throws Exception {
    return makeURI(which, get(which));
  }

  // Create a new submit ad from URL strings.
  public SubmitAd(String s, String d) throws Exception {
    src  = makeURI("src", s);  src_proto  = src.getScheme();
    dest = makeURI("dest", d); dest_proto = dest.getScheme();
  } 

  // Create a submit ad from a Ad.
  public SubmitAd(Ad ad) throws Exception {
    super(ad.trim());

    // Do some translation.
    rename("src_url", "src");
    rename("dest_url", "dest");

    String p = require("src", "dest");

    if (p != null)
      throw new Exception("missing parameter: "+p);

    src  = makeURI("src");  src_proto  = src.getScheme();
    dest = makeURI("dest"); dest_proto = dest.getScheme();
  }

  public void setModule(TransferModule tm) throws Exception {
    Ad ad1 = filter(params);
    Ad ad2 = tm.validateAd(this);

    merge(ad2);
    put("module", tm.handle());

    this.tm = tm;
  }
}
