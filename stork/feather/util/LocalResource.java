package stork.feather.util;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

import stork.feather.*;

/** A {@code Resource} produced by a {@code LocalSession}. */
public class LocalResource extends Resource<LocalSession,LocalResource> {
  // Separate reference to work around javac bug.
  LocalSession session;

  public LocalResource(LocalSession session, Path path) {
    super(session, path);
    this.session = session;
  }

  // Get the absolute path to the file.
  private Path path() {
    return path.absolutize(session.path);
  }

  // Get the File based on the path.
  public File file() {
    Path p = session.path.append(path);
    return new File(p.toString());
  }

  public Bell<LocalResource> mkdir() {
    return session.new TaskBell() {
      public void task() {
        File file = file();
        if (file.exists() && file.isDirectory())
          throw new RuntimeException("Resource is a file.");
        else if (!file.mkdirs())
          throw new RuntimeException("Could not create directory.");
      }
    }.thenAs(this).detach();
  }

  public Bell<LocalResource> unlink() {
    return session.new TaskBell() {
      private File root = file();

      public void task() {
        remove(root);
      }

      // Recursively remove files.
      private void remove(File file) {
        if (isCancelled()) {
          throw new java.util.concurrent.CancellationException();
        } if (!file.exists()) {
          if (file == root)
            throw new RuntimeException("Resource does not exist: "+file);
        } else try {
          // If not a symlink and is a directory, delete contents.
          if (file.getCanonicalFile().equals(file) && file.isDirectory())
            for (File f : file.listFiles()) remove(f);
          if (!file.delete() && file == root)
            throw new RuntimeException("Resource could not be deleted: "+file);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }.thenAs(this).detach();
  }

  // Returns the target File if this is a symlink, null otherwise.
  private File resolveLink(File file) {
    try {
      File cf = file.getCanonicalFile();
      if (!file.equals(cf))
        return cf;
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  public Bell<Stat> stat() {
    return session.new TaskBell() {
      public void task() {
        File file = file();

        if (!file.exists())
          throw new RuntimeException("Resource does not exist: "+file);

        Stat stat = new Stat(file.getName());
        stat.size = file.length();
        stat.file = file.isFile();
        stat.dir = file.isDirectory();
        
        File sym = resolveLink(file);
        if (sym != null)
          stat.link = Path.create(file.toString());

        if (stat.dir) stat.setFiles(file.list());
        stat.time = file.lastModified();

        ring(stat);
      }
    }.detach();
  }

  public Tap<LocalResource> tap() {
    return new LocalTap(this);
  }
}

class LocalTap extends Tap<LocalResource> {
  // Small hack to take advantage of NIO features.
  private WritableByteChannel nioToFeather = new WritableByteChannel() {
    public int write(ByteBuffer buffer) {
      Slice slice = new Slice(buffer);
      drain(currentResource.wrap(slice));
      return slice.length();
    }

    public void close() { }
    public boolean isOpen() { return true; }
  };

  private long chunkSize = 16384;

  // State of the current transfer.
  private Relative<LocalResource> currentResource;
  private FileChannel currentChannel;
  private long offset = 0, remaining = 0;
  private Bell pauseBell;

  public LocalTap(LocalResource root) { super(root, false); }

  // Because this is a passive tap, the bell needs to have a handler that
  // begins transfer when it rings.
  public Bell<LocalResource> initialize(final Relative<LocalResource> r) {
    System.out.println("Initializing: "+r.object);
    File file = r.object.file();

    if (!file.exists())
      throw new RuntimeException("File not found");
    if (!file.canRead())
      throw new RuntimeException("Permission denied");
    return new Bell<LocalResource>() {
      public void done(LocalResource lr) { beginTransferFor(r); }
    };
  }

  // Called after initialization.
  private void beginTransferFor(Relative<LocalResource> r) {
    File file = r.object.file();

    // If it's not a data file, we're done.
    if (!file.isFile()) {
      finalize(r);
      return;
    }

    try {
      // Set up state.
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      currentChannel = raf.getChannel();
      currentResource = r;
      offset = 0;
      remaining = file.length();
      sendChunk();
    } catch (Exception e) {
      // Ignore for now...
      e.printStackTrace();
    }
  }

  // Send the next chunk. This method keeps calling itself asynchronously until
  // the file has been sent.
  public synchronized void sendChunk() {
    System.out.println("Sending");
    // If paused, delay until resumed.
    if (pauseBell != null) pauseBell.new Promise() {
      public void done() { sendChunk(); }
    };

    // See if we're done.
    else if (remaining <= 0)
      finalize(currentResource);

    // Submit a task to send the next chunk.
    else root.session.new TaskBell() {
      public void task() throws Exception {
        System.out.println("Sending: "+offset);
        long len = remaining < chunkSize ? remaining : chunkSize;
        len = currentChannel.transferTo(offset, len, nioToFeather);
        offset += len;
        remaining -= len;
      } public void done() {
        sendChunk();
      }
    };
  }

  public synchronized void pause() {
    pauseBell = new Bell();
  }

  public synchronized void resume() {
    pauseBell.ring();
    pauseBell = null;
  }
}
