package stork.module;

import java.net.*;
import java.util.concurrent.*;

import stork.util.*;

// A source, sink, or conduit for data. Typically a channel represents a
// remote file resource, but can also be a conduit which redirects and
// optionally transforms remote data on its way to another channel. A
// channel can both or either send data to or receive data from another
// channel.

public abstract class StorkChannel {
  public final String base;
  public final FileTree file;

  public StorkChannel(String base, FileTree ft) {
    file = ft;
    this.base = base;
  }

  // Get the size of the resource represented by this channel, or return
  // -1 if it is infinite or unknown.
  public long size() {
    return file.size;
  }

  // Check if the channel endpoint already exists.
  public abstract boolean exists();

  // Send data to another channel. The two channels should coordinate to
  // produce a bell that will be rung when the exchange is complete. A
  // matching recvFrom should be called on the other channel. Sending
  // with a length of -1 will cause data to be sent until the source is
  // out of data.
  public final Future<?> sendTo(StorkChannel c) {
    return sendTo(c, 0, -1);
  } public abstract Future<?> sendTo(StorkChannel c, long off, long len);

  // Close the channel, finalizing any resources held by the channel,
  // and ending any ongoing transfers.
  public abstract void close();
}
