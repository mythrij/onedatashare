package stork.module.ftp;

public class FTPSession extends StorkSession {

  // Perform a listing of the given path.
  protected abstract Bell.Out<FileTree> listImpl(String path) {
    
  }

  // Get the size of a file given by a path.
  protected abstract Bell.Out<Long> sizeImpl(String path);

  // Create a directory at the end-point, as well as any parent directories.
  protected abstract Bell.Out<?> mkdirImpl(String path);

  // Remove a file or directory.
  protected abstract Bell.Out<?> rmImpl(String path);

  // Close the session and free any resources.
  protected abstract void closeImpl();

  // Create an identical session with the same settings.
  //public abstract StorkSession duplicate();

  // Get a channel to a session resource.
  protected abstract StorkChannel openImpl(String base, FileTree ft);
}
