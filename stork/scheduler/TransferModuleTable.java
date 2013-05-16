package stork.scheduler;

import stork.ad.*;
import stork.module.*;
import java.util.*;

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

  // Add a transfer module to the table.
  public void register(TransferModule tm) {
    if (tm == null) {
      System.out.println("Error: register called with null argument");
      return;
    }

    // Check if handle is in use.
    if (!by_handle.containsKey(tm.handle())) {
      by_handle.put(tm.handle(), tm);
      System.out.println(
        "Registered module \""+tm+"\" [handle: "+tm.handle()+"]");
    } else {
      System.out.println(
        "Warning: module handle "+tm.handle()+" in use, ignoring");
      return;
    }

    // Add the protocols for this module.
    for (String p : tm.protocols()) {
      if (!by_proto.containsKey(p)) {
        System.out.println("  Registering protocol: "+p);
        by_proto.put(p, tm);
      } else {
        System.out.println(
          "  Note: protocol "+p+" already registered, not registering");
        continue;
      }
    }
  }

  // Get a transfer module by its handle.
  public TransferModule byHandle(String handle) {
    return by_handle.get(handle);
  }

  // Get a transfer module by protocol.
  public TransferModule byProtocol(String p) {
    return by_proto.get(p);
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
