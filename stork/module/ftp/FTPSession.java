package stork.module.ftp;

import io.netty.buffer.*;

import stork.cred.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

public class FTPSession extends FTPResource implements Session {
  transient FTPChannel ch;

  // Transient state related to the channel configuration.
  private transient boolean mlstOptsAreSet = false;

  // Create an FTP session given the passed endpoint.
  private FTPSession(Endpoint e) {
    super(e.uri);
    ch = new FTPChannel(e.uri);
  }

  // Asynchronously establish the session.
  public static Bell<FTPSession> connect(String uri) {
    return connect(new Endpoint(uri));
  }

  public static Bell<FTPSession> connect(Endpoint e) {
    final Bell<FTPSession> bell = new Bell<FTPSession>();
    final FTPSession sess = new FTPSession(e);
    String user = "anonymous";
    String pass = "";

    // Pull userinfo from URI.
    if (e.uri.username() != null)
      user = e.uri.username();
    if (e.uri.password() != null)
      pass = e.uri.password();

    if (e.credential == null) {
      // Do nothing.
    } else if (e.credential instanceof StorkGSSCred) try {
      StorkGSSCred cred = (StorkGSSCred) e.credential;
      sess.ch.authenticate(cred.data()).sync();
      user = ":globus-mapping:";  // FIXME: This is GridFTP-specific.
    } catch (Exception ex) {
      // Couldn't authenticate with the given credentials...
      throw new RuntimeException(ex);
    } else if (e.credential instanceof StorkUserinfo) {
      StorkUserinfo cred = (StorkUserinfo) e.credential;
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
  public Bell<Void> close() {
    return new Bell<Void>().ring();
  }

  public static void main(String[] args) {
    FTPSession src  = connect("ftp://didclab-ws8/home/globus/.bash_history").sync();
    FTPSession dest = connect("ftp://didclab-ws8/home/globus/small2.txt").sync();
    src.transferTo(dest);
  }
}
