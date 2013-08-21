package stork.module.sftp;

import stork.ad.*;
import stork.util.*;
import stork.cred.*;
import stork.module.*;
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
  public SFTPSession(URI uri, Ad opts) {
    super(uri, opts);

    int port = uri.getPort();
    if (port < 0)
      port = DEFAULT_PORT;

    // TODO: Check for credentials in ad.
    final String[] ui = StorkUserinfo.split(uri.getUserInfo());

    // Establish the session connection. TODO: scp vs. sftp
    try {
      jsch = new JSch();
      JSch.setConfig("StrictHostKeyChecking", "no");

      session = jsch.getSession(ui[0], uri.getHost(), port);
      session.setUserInfo(new UserInfo() {
        public String getPassphrase() { return null; }
        public String getPassword() { return ui[1]; }
        public boolean promptPassphrase(String m) { return true; }
        public boolean promptPassword(String m) { return true; }
        public boolean promptYesNo(String m) { return true; }
        public void showMessage(String m) { throw new FatalEx(m); }
      });
      session.setDaemonThread(true);
      session.connect();

      channel = (ChannelSftp) session.openChannel("sftp");
      channel.connect();
    } catch (Exception e) {
      throw new FatalEx("error connecting", e);
    }
  }

  // Get a directory listing of a path from the session.
  protected Ad listImpl(String path, Ad opts) {
    try {
      Ad ad = new Ad("name", path);
      AdSorter sorter = new AdSorter("dir", "name");

      for (Object o : channel.ls(path)) {
        ChannelSftp.LsEntry ls = (ChannelSftp.LsEntry) o;
        String name = ls.getFilename();
        boolean dir = ls.getAttrs().isDir();
        long size   = ls.getAttrs().getSize();

        // Ignore certain names.
        if (name == null || name.equals(".") || name.equals(".."))
          continue;

        Ad a = new Ad("name", name);

        if (dir) {
          ad.put("dir", true);
        } else {
          a.put("file", true);
          a.put("size", size);
        }

        // Add file to list.
        sorter.add(a);
      }

      return ad.put("files", sorter.getAds());
    } catch (Exception e) {
      throw new FatalEx("could not list: "+path, e);
    }
  }

  // Get the size of a file given by a path.
  protected long sizeImpl(String path) {
    return -1;
  }

  // Create a directory at the end-point, as well as any parent directories.
  // Returns whether or not the command succeeded.
  //protected abstract boolean mkdirImpl(String path);

  // Transfer from this session to a paired session. opts can be null.
  protected void transferImpl(String src, String dest, Ad opts) {
    throw new FatalEx("sftp transfer is not supported");
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
  public static void main(String[] args) {
    try {
      URI u = new URI(args[0]);
      SFTPSession sess = new SFTPSession(u, null);
      System.out.println(sess.list(u.getPath()));
      sess.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Usage: SFTPSession <url>");
      System.exit(1);
    }
  }
}
