package stork.feather.util;

import java.io.*;
import java.nio.*;
import java.nio.channels.*;

import stork.feather.*;

/**
 * A {@code Resource} on the local file system. This is intended to serve as an
 * example Feather implementation, but can also be used for testing other
 * implementations.
 * <p/>
 * Many of the methods in this implementation perform long-running operations
 * concurrently using threads because this is the most straighforward way to
 * demonstrate the asynchronous nature of session operations. However, this is
 * often not the most efficient way to perform operations concurrently, and
 * ideal implementations would use an alternative method.
 */
public class LocalResource extends Resource<LocalSession,LocalResource> {
  public LocalResource(LocalSession session, Path path) {
    super(session, path);
  }

  // Get the absolute path to the file.
  private Path path() {
    return path.absolutize(session.path);
  }

  // Get the File based on the path.
  protected File file() {
    return new File(path().toString());
  }

  public Bell mkdir() {
    return session.new TaskBell() {
      public void task() {
        File file = file();
        if (file.exists() && file.isDirectory())
          throw new RuntimeException("Resource is a file.");
        else if (!file.mkdirs())
          throw new RuntimeException("Could not create directory.");
      }
    };
  }

  public Bell unlink() {
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
            throw new RuntimeException("Resource does not exist.");
        } else try {
          // If not a symlink and is a directory, delete contents.
          if (file.getCanonicalFile().equals(file) && file.isDirectory())
            for (File f : file.listFiles()) remove(f);
          if (!file.delete() && file == root)
            throw new RuntimeException("Resource could not be deleted.");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  public Bell<Stat> stat() {
    return session.new TaskBell<Stat>() {
      public void task() {
        File file = file();

        if (!file.exists())
          throw new RuntimeException("Resource does not exist.");

        Stat stat = new Stat(file.getName());
        stat.size = file.length();
        stat.setFiles(file.list());
        stat.time = file.lastModified();

        ring(stat);
      }
    };
  }

  public Tap<LocalResource> tap() {
    return new LocalTap(this);
  }
}

// A passive Tap that will use a Pump to send files recursively.
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
    if (!file.isFile())
      return;

    try {
      // Set up state.
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      currentChannel = raf.getChannel();
      currentResource = r;
      offset = 0;
      remaining = file.length();

    } catch (Exception e) {
      // Ignore for now...
      e.printStackTrace();
    }
  }

  // Send the next chunk. This method keeps calling itself asynchronously until
  // the file has been sent.
  public synchronized void sendChunk() {
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
