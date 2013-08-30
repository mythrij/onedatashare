package stork.scheduler;

import stork.ad.*;
import stork.module.*;
import stork.util.*;
import static stork.util.StorkUtil.join;

import java.util.*;
import java.io.*;

// A class for looking up transfer modules by protocol and handle.

public class TransferModuleTable {
  private static TransferModuleTable instance = null;
  Map<String, TransferModule> by_proto, by_handle;

  public TransferModuleTable() {
    by_proto  = new HashMap<String, TransferModule>();
    by_handle = new HashMap<String, TransferModule>();
  }

  // Get the global transfer module table.
  public static synchronized TransferModuleTable instance() {
    if (instance == null)
      instance = new TransferModuleTable();
    return instance;
  }

  // Add a directory of executables to the transfer module table.
  // TODO: Do this in parallel and detect misbehaving externals.
  public void registerDirectory(File dir) {
    if (!dir.isDirectory()) {
      Log.warning('"', dir, "\" is not a directory!");
    } else for (File f : dir.listFiles()) {
      // Skip over things that obviously aren't transfer modules.
      if (f.isFile() && !f.isHidden() && f.canExecute())
        register(f);
    }
  }

  // Add a transfer module to the table.
  public void register(File f) {
    register(new ExternalModule(f));
  } public void register(TransferModule tm) {
    // Check if handle is in use.
    if (!by_handle.containsKey(tm.handle)) {
      by_handle.put(tm.handle, tm);
      Log.info("Registered module \"", tm, "\" [handle: ", tm.handle, "]");
    } else {
      Log.warning("Module handle \"", tm.handle, "\"in use, ignoring");
      return;
    }

    // Add the protocols for this module.
    Set<String> good = new TreeSet<String>(), bad = new TreeSet<String>();
    for (String p : tm.protocols) {
      if (by_proto.get(p) == null) {
        good.add(p);
        by_proto.put(p, tm);
      } else {
        bad.add(p);
      }
    }

    if (!good.isEmpty())
      Log.info("  Registering protocol(s): ", join(good.toArray()));
    if (!bad.isEmpty())
      Log.info("  Protocols already registered: ", join(bad.toArray()));
  }

  // Get all of the transfer module info ads in a list.
  public Ad infoAds() {
    Ad mods = new Ad();
    for (TransferModule tm : by_handle.values())
      mods.put(tm.handle, Ad.marshal(tm));
    return mods;
  }

  // Get a transfer module by its handle.
  public TransferModule byHandle(String h) {
    TransferModule tm = by_handle.get(h);
    if (tm != null)
      return tm;
    throw new RuntimeException("no module '"+h+"' registered");
  }

  // Get a transfer module by protocol.
  public TransferModule byProtocol(String p) {
    TransferModule tm = by_proto.get(p);
    if (tm != null)
      return tm;
    throw new RuntimeException("no module for protocol '"+p+"' registered");
  }

  // Get a set of all the modules.
  public Collection<TransferModule> modules() {
    return by_handle.values();
  }

  // Get a set of all the handles.
  public Collection<String> handles() {
    return by_handle.keySet();
  }

  // Get a set of all the supported protocols.
  public Collection<String> protocols() {
    return by_proto.keySet();
  }
}
