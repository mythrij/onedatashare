package stork.module.sftp;

import stork.ad.*;
import stork.util.*;
import stork.cred.*;
import stork.module.*;
import stork.scheduler.*;
import static stork.module.ModuleException.*;

import java.util.*;
import java.net.URI;
import com.jcraft.jsch.*;

// Represents a connection to a remote end-point. A session should
// provide methods for starting a transfer, listing directories, and
// performing other operations on the end-point.

public class SFTPSession extends StorkSession {
  public static int DEFAULT_PORT = 22;

  private JSch jsch;
  private Session session;
  private ChannelSftp channel;

  // Create a new connection from a URI and options.
  public SFTPSession(EndPoint e) {
    super(e);
    URI uri = e.uri()[0];

    int port = uri.getPort();
    if (port < 0)
      port = DEFAULT_PORT;

    // TODO: Check for credentials in ad.
    final String[] ui = StorkUserinfo.split(uri.getUserInfo());

    // Establish the session connection. TODO: scp vs. sftp
    jsch = new JSch();
    JSch.setConfig("StrictHostKeyChecking", "no");

    try {
      session = jsch.getSession(ui[0], uri.getHost(), port);
      session.setUserInfo(new UserInfo() {
        public String getPassphrase() { return null; }
        public String getPassword() { return ui[1]; }
        public boolean promptPassphrase(String m) { return true; }
        public boolean promptPassword(String m) { return true; }
        public boolean promptYesNo(String m) { return true; }
        public void showMessage(String m) { throw abort(m); }
      });
      session.setDaemonThread(true);
      session.connect();

      channel = (ChannelSftp) session.openChannel("sftp");
      channel.connect();
    } catch (Exception ex) {
      throw abort(ex);
    }
  }

  protected StorkChannel openImpl(String base, FileTree ft) {
    return null;
  }

  // Get a directory listing of a path from the session.
  protected Bell.Single<FileTree> listImpl(String path, Ad opts) {
    FileTree ft = new FileTree(StorkUtil.basename(path));
    ft.dir = true;
    List<FileTree> sub = new LinkedList<FileTree>();

    try {
      for (Object o : channel.ls(path)) {
        ChannelSftp.LsEntry ls = (ChannelSftp.LsEntry) o;

        String name = ls.getFilename();
        FileTree sft = new FileTree(name);
        sft.dir  = ls.getAttrs().isDir();
        sft.file = !sft.dir;
        sft.size = ls.getAttrs().getSize();

        // Ignore certain names.
        if (name == null || name.equals(".") || name.equals(".."))
          continue;
        sub.add(sft);
      }

      ft.setFiles(sub);
      Bell.Single<FileTree> bell = new Bell.Single<FileTree>();
      bell.ring(ft);
      return bell;
    } catch (Exception e) {
      throw abort(e);
    }
  }

  // Get the size of a file given by a path.
  protected Bell.Single<Long> sizeImpl(String path) {
    return new Bell.Single<Long>(-1l);
  }

  protected Bell.Single<?> mkdirImpl(String path) {
    return null;
  }

  protected Bell.Single<?> rmImpl(String path) {
    return null;
  }

  // Create a directory at the end-point, as well as any parent directories.
  // Returns whether or not the command succeeded.
  //protected abstract boolean mkdirImpl(String path);

  // Transfer from this session to a paired session. opts can be null.
  protected void transferImpl(String src, String dest, Ad opts) {
    throw abort("sftp transfer is not supported");
  }

  // Close the session and free any resources.
  protected void closeImpl() {
    channel.disconnect();
    session.disconnect();
  }

  // Create an identical session with the same settings. Can optionally
  // duplicate its pair as well and pair the duplicates.
  //public abstract StorkSession duplicate(boolean both);

  // Check if this session can be paired with another session. Override
  // this in subclasses.
  public boolean pairCheck(StorkSession other) {
    return (other instanceof SFTPSession);
  }

  // Testing code.
  public static void main(String[] args) throws Exception {
    URI u = new URI(args[0]);
    SFTPSession sess = new SFTPSession(new EndPoint(u));
    System.out.println(Ad.marshal(sess.list(u.getPath()).get()));
    sess.close();
  }
}
