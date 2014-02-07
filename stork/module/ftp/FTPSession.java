package stork.module.ftp;

import io.netty.buffer.*;

import stork.cred.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

public class FTPSession extends Session {
  private transient FTPChannel ch;

  // Transient state related to the channel configuration.
  private transient boolean mlstOptsAreSet = false;

  // Create an FTP session given the passed endpoint.
  private FTPSession(Endpoint e) {
    super(e);
    ch = new FTPChannel(e.uri[0]);
  }

  // Asynchronously establish the session.
  public static Bell<FTPSession> connect(Endpoint e) {
    final Bell<FTPSession> bell = new Bell<FTPSession>();
    final FTPSession sess = new FTPSession(e);
    String user = "anonymous";
    String pass = "";

    if (e.cred == null) {
      // Do nothing.
    } else if (e.cred instanceof StorkGSSCred) try {
      StorkGSSCred cred = (StorkGSSCred) e.cred;
      sess.ch.authenticate(cred.data()).sync();
      user = ":globus-mapping:";  // FIXME: This is GridFTP-specific.
    } catch (Exception ex) {
      // Couldn't authenticate with the given credentials...
      throw new RuntimeException(ex);
    } else if (e.cred instanceof StorkUserinfo) {
      StorkUserinfo cred = (StorkUserinfo) e.cred;
      user = cred.getUser();
      pass = cred.getPass();
    }

    sess.ch.authorize(user, pass).promise(new Bell() {
      protected void done() {
        bell.ring(sess);
      } protected void fail(Throwable t) {
        bell.ring(t);
      }
    });

    return bell;
  }

  // Perform a listing of the given path relative to the root directory.  There
  // are the supported listing commands in terms of preference:
  //   MLSC STAT MLSD LIST
  public synchronized Bell<Stat> stat(final String path) {
    if (path.startsWith("/~"))
      return stat("~"+path.substring(2));
    if (!path.startsWith("/") && !path.startsWith("~"))
      return stat("/"+path);
    if (ch.supports("MLSC").sync())
      return goList(true, "MLSC", path);
    if (ch.supports("STAT").sync())
      return goList(true, "STAT", path);
    if (ch.supports("MLSD").sync())
      return goList(false, "MLSD", path);
    if (ch.supports("LIST").sync())
      return goList(false, "LIST", path);
    return new Bell<Stat>().ring(
      new Exception("Listing is unsupported."));
  }

  // This method will initiate a listing using the given command.
  private Bell<Stat> goList(
      boolean cc, final String cmd, final String path) {
    final Bell<Stat> bell = new Bell<Stat>();
    final char hint = cmd.startsWith("M") ? 'M' : 0;
    final FTPListParser parser = new FTPListParser(null, hint);

    // When doing MLSx listings, we can reduce the response size with this.
    if (hint == 'M' && !mlstOptsAreSet) {
      ch.new Command("OPTS MLST Type*;Size*;Modify*;UNIX.mode*");
      mlstOptsAreSet = true;
    }

    // Do a control channel listing, if specified.
    if (cc) ch.new Command(cmd, path) {
      public void handle(FTPChannel.Reply r) {
        if (r.code/100 > 2)
          bell.ring(r.asError());
        parser.write(r.message().getBytes());
        if (r.isComplete())
          bell.ring(parser.finish());
      }
    };

    // Otherwise we're doing a data channel listing. This requires a command
    // sequence, so we need a locked channel.
    else ch.lock().promise(new Bell<FTPChannel>() {
      protected void done(final FTPChannel ch) {
        ch.new DataChannel() {
          public void handle(ByteBuf b) {
            System.out.println("GOT: "+b.toString(ch.data.encoding));
            parser.write(b);
          } public void done() {
            bell.ring(parser.finish());
          }
        };
        ch.new Command(cmd, path);
        ch.unlock();
      }
    });

    return bell;
  }

  // Create a directory at the end-point, as well as any parent directories.
  public Bell<Void> mkdir(String path) {
    return null;
  }

  // Remove a file or directory.
  public Bell<Void> rm(String path) {
    return null;
  }

  // Close the session and free any resources.
  public void close() {
    //ch.close();
  }

  // Select a resource given the path part of the URI.
  public Resource select(final URI uri) {
    return new Resource() {
      public FTPSession session() {
        return FTPSession.this;
      }

      // Detect if we're able to do a third-party transfer. If not, just do a
      // proxy transfer.
      public Transfer transferTo(Resource r) {
        return super.transferTo(r);
      }

      public URI uri() {
        return uri;
      }

      public Sink sink() {
        return ch.new DataChannel();
      }

      public Tap tap() {
        return ch.new DataChannel();
      }
    };
  }

  // Throws an error if the session is closed.
  private void checkSession() {
  }

  public static void main(String[] args) {
    String u = (args.length == 0) ? "ftp://didclab-ws8/" : args[1];
    //String u = (args.length == 0) ? "ftp://ftp.cse.buffalo.edu/" : args[1];
    FTPSession sess = connect(new Endpoint(u)).sync();
    String[] paths = { "/", "~", "/etc", "/tmp", "/var/run", "/bad/path" };
    for (final String p : paths) {
      sess.stat(p).promise(new Bell<Stat>() {
        protected void done(Stat t) {
          System.out.println(stork.ad.Ad.marshal(t));
        } protected void fail(Throwable t) {
          System.out.println("Failed to list: "+p+"  "+t);
        }
      });
    }
  }
}
