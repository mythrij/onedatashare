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
    super(e.uri[0]);
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

  // Perform a listing of the given path relative to the root directory.
  public synchronized Bell<Stat> stat(final String path) {
    if (path.startsWith("/~"))
      return stat(path.substring(1));
    if (!path.startsWith("/") && !path.startsWith("~"))
      return stat("/"+path);

    if (ch.supports("MLSC").sync())
      return goList(true, "MLSC", path);
    if (ch.supports("STAT").sync())
      return goList(true, "STAT", path);
    if (ch.supports("MLSD").sync())
      return goList(false, "MLSD", path);
    return goList(false, "LIST", path);
  }

  // This method will initiate a listing using the given command.
  private Bell<Stat> goList(
      boolean cc, final String cmd, final String path) {
    final char hint = cmd.startsWith("M") ? 'M' : 0;
    final FTPListParser parser = new FTPListParser(null, hint);

    parser.name(StorkUtil.basename(path));

    // When doing MLSx listings, we can reduce the response size with this.
    if (hint == 'M' && !mlstOptsAreSet) {
      ch.new Command("OPTS MLST Type*;Size*;Modify*;UNIX.mode*");
      mlstOptsAreSet = true;
    }

    // Do a control channel listing, if specified.
    if (cc) ch.new Command(cmd, path) {
      public void handle(FTPChannel.Reply r) {
        if (r.code/100 > 2)
          parser.ring(r.asError());
        parser.write(r.message().getBytes());
        if (r.isComplete())
          parser.finish();
      }
    };

    // Otherwise we're doing a data channel listing.
    else ch.new DataChannel() {{
      attach(parser);
      new Command(cmd, path);
      unlock();
    }};

    return parser;
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
    return new Resource(uri) {
      public Session session() {
        return FTPSession.this;
      }

      // Detect if we're able to do a third-party transfer. If not, just do a
      // proxy transfer.
      public Transfer transferTo(Resource r) {
        return super.transferTo(r);
      }

      public Sink sink() {
        return ch.new DataChannel() {{
          new Command("STOR", uri.path());
          unlock();
        }};
      }

      public Tap tap() {
        return ch.new DataChannel() {{
          new Command("RETR", uri.path());
          unlock();
        }};
      }
    };
  }

  // Open a tap to the session root.
  public Tap tap() {
    return select(uri).tap();
  }

  // Open a sink to the session root.
  public Sink sink() {
    return select(uri).sink();
  }

  // Throws an error if the session is closed.
  private void checkSession() {
  }

  public static void main(String[] args) {
    String u = "ftp://didclab-ws8/dev/urandom";
    FTPSession sess = connect(new Endpoint(u)).sync();
    sess.tap().attach(new Sink() {
      public void write(Slice s) {
        System.out.println(s);
      } public void write(ResourceError e) {
        System.out.println(e);
      }
    });
  }
}
