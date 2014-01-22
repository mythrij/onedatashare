package stork.module.ftp;

import stork.cred.*;
import stork.module.*;
import stork.scheduler.*;
import stork.util.*;

public class FTPSession extends StorkSession {
  private transient FTPChannel ch;

  // Transient state related to the channel configuration.
  private transient boolean mlstOptsAreSet = false;

  // Create an FTP session given the passed endpoint.
  private FTPSession(EndPoint e) {
    super(e);
    ch = new FTPChannel(e.uri[0]);
  }

  // Asynchronously establish the session.
  public static Bell<FTPSession> connect(EndPoint e) {
    final Bell<FTPSession> bell = new Bell<FTPSession>();
    final FTPSession sess = new FTPSession(e);
    String user = "anonymous";
    String pass = "";

    if (e.cred == null) {
      // Do nothing.
    } else if (e.cred instanceof StorkGSSCred) try {
      StorkGSSCred cred = (StorkGSSCred) e.cred;
      sess.ch.authenticate(cred.credential()).sync();
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
  // Listing in FTP is definitive proof that evil truly exists in this world,
  // and you will see it reflected in this function. Different implementations
  // not only present listing in different formats, but have different
  // semantics for different list command arguments. We will make an attempt to
  // sort out the mess as efficiently as possible, but it will not be pretty.
  // Beware, all ye who enter, for here be dragons.
  public synchronized Bell<FileTree> list(final String path) {
    if (!path.startsWith("/") && !path.startsWith("~"))
      return list("/"+path);
    final Bell<FileTree> bell = new Bell<FileTree>();

    // First, test if the server supports the MLSx family of listing commands.
    // If so, we can also control the format of the resulting list and reduce
    // the response size.
    if (!mlstOptsAreSet) ch.supportsAny("MLST", "MLSD", "MLSC").promise(
      new Bell<Boolean>() {
        protected void handle(Boolean b) {
          if (!b) return;
          if (!mlstOptsAreSet)
            ch.new Command("OPTS MLST Type*;Size*;Modify*;UNIX.mode*");
          mlstOptsAreSet = true;
        }
      }
    );

    if (ch.supports("MLSC").sync())
      return goList(true, "MLSC", path);
    if (ch.supports("STAT").sync())
      return goList(true, "STAT", path);
    bell.ring(new Exception("Listing is unsupported."));

    return bell;
  }

  // This method will initiate a listing using the given command.
  private Bell<FileTree> goList(boolean cc, final String cmd, String path) {
    final Bell<FileTree> bell = new Bell<FileTree>();
    final char hint = cmd.startsWith("M") ? 'M' : 0;

    // Do a control channel listing, if specified.
    if (cc) ch.new Command(cmd, path) {
      FTPListParser p = new FTPListParser(null, hint);
      public void handle(FTPChannel.Reply r) {
        if (r.code/100 > 2)
          bell.ring(r.asError());
        p.write(r.message().getBytes());
        if (r.isComplete())
          bell.ring(p.finish());
      }
    };

    else;  // DC listing.

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

  // Get a channel to a session resource.
  public StorkChannel open(String base, FileTree ft) {
    return null;
  }

  // Throws an error if the session is closed.
  private void checkSession() {
  }

  public static void main(String[] args) {
    //String u = (args.length == 0) ? "ftp://didclab-ws8/" : args[1];
    String u = (args.length == 0) ? "ftp://ftp.cse.buffalo.edu/" : args[1];
    FTPSession sess = connect(new EndPoint(u)).sync();
    String[] paths = { "/", "~", "/etc", "/tmp", "/var/run", "/bad/path" };
    for (final String p : paths) {
      sess.list(p).promise(new Bell<FileTree>() {
        protected void done(FileTree t) {
          System.out.println(stork.ad.Ad.marshal(t));
        }
      });
    }
  }
}
