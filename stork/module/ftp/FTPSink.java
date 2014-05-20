package stork.module.ftp;

import stork.feather.*;

/**
 * A FTP {@code Sink} which manages data channels autonomonously.
 */
public class FTPSink extends Sink<FTPResource> {
  // Current data channel.
  private FTPChannel.DataChannel dc = null;

  public FTPSink(FTPResource resource) {
    super(resource);
  }

  // Just make sure the control channel is connected.
  public Bell<FTPSink> start() {
    return root.initialize().thenAs(this);
  }

  // Create a new data channel for every initialized resource.
  public Bell<FTPResource> initialize(Relative<FTPResource> resource) {
    final Relative<FTPResource> r = resource;
    return r.origin.stat().new ThenAs<FTPResource>() {
      public void then(Stat stat) {
        if (stat.dir) {
          r.object.mkdir().promise(this);
        } else if (stat.file) {
          dc = root.session.channel.new DataChannel() {
            public void init() {
              new Command("STOR", r.path);
            }
          };
          dc.onConnect().thenAs(root).promise(this);
        }
      }
    };
  }

  public void drain(final Relative<Slice> slice) {
    dc.onConnect().new Promise() {
      public void done(FTPChannel.DataChannel d) { d.send(slice.object); }
    };
  }
}
