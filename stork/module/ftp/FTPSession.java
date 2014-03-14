package stork.module.ftp;

import io.netty.buffer.*;

import stork.cred.*;
import stork.feather.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

public class FTPSession extends Session {
  transient FTPChannel ch;

  // Transient state related to the channel configuration.
  private transient boolean mlstOptsAreSet = false;

  public FTPSession(URI uri, Credential cred) {
    super(uri, cred);
  }

  public Bell<Void> open() {
    final Bell<Void> bell = new Bell<Void>();
    String user = "anonymous";
    String pass = "";

    ch = new FTPChannel(uri) {
      protected void onClose() { FTPSession.this.close(); }
    };

    // Pull userinfo from URI.
    if (uri.username() != null)
      user = uri.username();
    if (uri.password() != null)
      pass = uri.password();

    if (credential == null) {
      // Do nothing.
    } else if (credential instanceof StorkGSSCred) try {
      StorkGSSCred cred = (StorkGSSCred) credential;
      ch.authenticate(cred.data()).sync();
      user = ":globus-mapping:";  // FIXME: This is GridFTP-specific.
    } catch (Exception ex) {
      // Couldn't authenticate with the given credentials...
      throw new RuntimeException(ex);
    } else if (credential instanceof StorkUserinfo) {
      StorkUserinfo cred = (StorkUserinfo) credential;
      user = cred.getUser();
      pass = cred.getPass();
    }

    ch.authorize(user, pass).promise(new Bell() {
      protected void done() {
        bell.ring();
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
  public Bell<Void> mkdir(URI uri) {
    return null;
  }

  // Remove a file or directory.
  public Bell<Void> rm(URI uri) {
    return null;
  }

  public Bell<Sink> sink(final URI uri) {
    return (Bell<Sink>) ch.new DataChannel() {{
      new Command("STOR", uri.path());
      unlock();
    }}.bell();
  }

  public Bell<Tap> tap(final URI uri) {
    return (Bell<Tap>) ch.new DataChannel() {{
      new Command("RETR", uri.path());
      unlock();
    }}.bell();
  }

  // Close the session and free any resources.
  public void doClose() {
    ch.close();
  }
}
