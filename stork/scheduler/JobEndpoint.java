package stork.scheduler;

import stork.feather.*;
import stork.module.*;

/**
 * An endpoint tied into the transfer module table to generate sessions.
 */
public class JobEndpoint extends Endpoint {
  public String module;

  public Session session() {
    TransferModuleTable tmt = TransferModuleTable.instance();
    TransferModule tm = null;
    if (module != null)
      tm = tmt.byHandle(module);
    else
      tm = tmt.byProtocol(uri.protocol());
    return (tm == null) ? null : tm.session(this);
  }
}
