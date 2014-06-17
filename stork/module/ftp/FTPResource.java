package stork.module.ftp;

import io.netty.buffer.*;

import stork.cred.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

public class FTPResource extends Resource<FTPSession, FTPResource> {
  FTPResource(FTPSession session, Path path) {
    super(session, path);
  }

  // TODO: If this is a non-singleton resource, use a crawler.
  public synchronized Bell<Stat> stat() {
    return initialize().new AsBell<Stat>() {
      private FTPChannel channel = null;

      // This will be called when initialization is done. It should start the
      // chain reaction that results in listing commands being tried until we
      // find one that works.
      public Bell<Stat> convert(FTPResource me) {
        channel = session.channel;
        tryListing(FTPListCommand.values()[0]);
        return null;
      }

      // This will call itself until it finds a supported command. If the
      // command is not supported, call itself again with the next available
      // command. If cmd is null, that means we've tried all commands.
      private void tryListing(final FTPListCommand cmd) {
        if (isDone()) {
          return;
        } if (cmd == null) {
          ring(new Exception("Listing is not supported."));
        } else channel.supports(cmd.toString()).new Promise() {
          public void done(Boolean supported) {
            if (supported)
              sendListCommand(cmd);
            else
              tryListing(cmd.next());
          }
        };
      }

      // This will get called once we've found a command that is supported.
      // However, if this fails, fall back to the next command.
      private void sendListCommand(final FTPListCommand cmd) {
        if (isDone())
          return;

        char hint = cmd.toString().startsWith("M") ? 'M' : 0;
        Stat base = new Stat(name());
        final Bell<Stat> tb = this;
        final FTPListParser parser = new FTPListParser(base, hint) {
          // The parser should ring this bell if it's successful.
          public void done(Stat stat) {
            tb.ring(stat);
          } public void fail() {
            // TODO: Check for permanent errors.
            tryListing(cmd.next());
          }
        };

        parser.name(StorkUtil.basename(path.name()));

        Log.fine("Trying list command: ", cmd);

        // When doing MLSx listings, we can reduce the response size with this.
        if (hint == 'M' && !session.mlstOptsAreSet) {
          channel.new Command("OPTS MLST Type*;Size*;Modify*;UNIX.mode*");
          session.mlstOptsAreSet = true;
        }

        // Do a control channel listing, if specified.
        if (!cmd.requiresDataChannel())
          channel.new Command(cmd.toString(), makePath()) {
            public void handle(FTPChannel.Reply r) {
              parser.write(r.message().getBytes());
            } public void done(FTPChannel.Reply r) {
              if (r.code/100 > 2) {
                parser.ring(r.asError());
              } else {
                parser.write(r.message().getBytes());
                parser.finish();
              }
            }
          };

        // Otherwise we're doing a data channel listing.
        else channel.new DataChannel() {
          public void init() {
            channel.new Command(cmd, makePath()) {
              public void done(FTPChannel.Reply r) {
                if (r.code/100 > 2) {
                  parser.ring(r.asError());
                  close();
                }
              }
            };
          } public void receive(Slice slice) {
            parser.write(slice.asBytes());
          } public void done() {
            parser.finish();
          }
        };
      }

      // Do this every time we send a command so we don't have to store a whole
      // concatenated path string for every outstanding listing command.
      private String makePath() {
        String p = path.toString();
        if (p.startsWith("/~"))
          return p.substring(1);
        else if (!p.startsWith("/") && !p.startsWith("~"))
          return "/"+p;
        return p;
      }
    };
  }

  // Create a directory at the end-point, as well as any parent directories.
  public Bell<FTPResource> mkdir() {
    if (!isSingleton())
      throw new UnsupportedOperationException();
    return initialize().new Promise() {
      public void then(FTPResource r) {
        session.channel.new Command("MKD", path).as(r).promise(this);
      }
    }.as(this);
  }

  // Remove a file or directory.
  public Bell<FTPResource> delete() {
    if (!isSingleton())
      throw new UnsupportedOperationException();
    return stat().new AsBell<FTPChannel.Reply>() {
      public Bell<FTPChannel.Reply> convert(Stat stat) {
        if (stat.dir)
          return session.channel.new Command("RMD", path);
        else
          return session.channel.new Command("DELE", path);
      }
    }.as(this);
  }

  public Sink<FTPResource> sink() {
    return new FTPSink(this);
  }

  public Tap<FTPResource> tap() {
    return new FTPTap(this);
  }
}

/**
 * An FTP {@code Tap} which manages data channels autonomonously.
 */
class FTPTap extends Tap<FTPResource> {
  private FTPChannel.DataChannel dc;

  public FTPTap(FTPResource resource) { super(resource); }

  protected Bell start(final Bell bell) {
    return bell.and(source().stat()).new Promise() {
      public void then(Stat stat) {
        if (stat.file) {
          // Source is a file, establish data channel and transfer.
          dc = source().session.channel.new DataChannel() {
            public void init() {
              new Command("RETR", source().path);
              ring();
            } public void receive(Slice slice) {  
              pauseUntil(drain(slice));
            }
          }.startWhen(bell);
        } else {
          throw new RuntimeException("Resource is a directory.");
        }
      }
    };
  }
}

/**
 * An FTP {@code Sink} which manages data channels autonomonously.
 */
class FTPSink extends Sink<FTPResource> {
  private FTPChannel.DataChannel dc;

  public FTPSink(FTPResource resource) { super(resource); }

  protected Bell start() {
    return source().stat().new Promise() {
      public void then(Stat stat) {
        if (stat.dir) {
          destination().mkdir().promise(this);
        } else if (stat.file) {
          dc = destination().session.channel.new DataChannel() {
            public void init() {
              new Command("STOR", destination().path);
              ring();
            }
          };
        } else {
          throw new RuntimeException("invalid source");
        }
      }
    };
  }

  public Bell drain(final Slice slice) {
    return dc.send(slice);
  }

  public void finish() { }
}
