package stork.module.dropbox;

import java.util.*;


import com.dropbox.core.v2.files.*;
import com.dropbox.core.DbxDownloader;
import stork.feather.*;
import stork.feather.errors.*;
import stork.feather.util.*;

public class DbxResource extends Resource<DbxSession, DbxResource> {
  DbxResource(DbxSession session, Path path) {
    super(session, path);
  }

  public synchronized Emitter<String> list() {
    final Emitter<String> emitter = new Emitter<>();
    new ThreadBell() {
      public Object run() throws Exception {
        ListFolderResult listing = session.client.files().listFolderContinue(path.toString());
        for (Metadata child : listing.getEntries())
          emitter.emit(child.getName());
        emitter.ring();
        return null;
      }
    }.startOn(initialize());
    return emitter;
  }

  public synchronized Bell<Stat> stat() {
    return new ThreadBell<Stat>() {
      public Stat run() throws Exception {

        ListFolderResult data = null;
        Metadata mData = null;
        /*Issue #21
          Modified by: Jimmy
          Comments: Dropbox v2 API uses "" as the root instead of "/". All other listings start with "/" */
        if(path.toString() == "/") {
          data = session.client.files().listFolder("");
        }
        else{
          try {
            data = session.client.files().listFolder(path.toString());
          }
          catch (ListFolderErrorException e){
            mData = session.client.files().getMetadata(path.toString());
          }
        }

        if (data == null && mData == null)
          throw new NotFound();
        //End changes for Issue #21

        Stat stat;
        if (data == null)
          stat = mDataToStat(mData);
        else {
          stat = mDataToStat(data.getEntries().iterator().next());
          /*Issue #25
		  	Modified by: Dhruva Kumar Srinivasa
		  	Comment: Fixed listing of folders. Set the selected directory as a directory due to Dropbox v2 API change.*/
          stat.dir = true;
          //End changes for Issue #25
        }

        stat.name = path.name();

        if (stat.dir) {
          ListFolderResult lfr = null;
          /*Issue #21
            Modified by: Jimmy
            Comments: Dropbox v2 uses "" as the root folder now instead of "/" */
          if(stat.name == "/") {
            lfr = session.client.files().listFolder("");
          }
          else{
            // If the metadata is a directory
            if(session.client.files().getMetadata(path.toString()) instanceof FolderMetadata) {
              // list the directory files
              lfr = session.client.files().listFolder(path.toString());
            }
            // If the metadata is a file
            else if(session.client.files().getMetadata(path.toString()) instanceof FileMetadata){
              // Return the metadata as a stat object
              return mDataToStat(session.client.files().getMetadata(path.toString()));
            }
          }
          //End changes for Issue #21
          List<Stat> sub = new LinkedList<>();
          for (Metadata child : lfr.getEntries())
            sub.add(mDataToStat(child));
          stat.setFiles(sub);
        }
        return stat;
      }
    }.startOn(initialize());
  }

  // Method that will take a Metadata object and create a Stat object
  private Stat mDataToStat(Metadata data) {
    Stat stat = new Stat(data.getName());
    if (data instanceof FileMetadata) {
      FileMetadata file = (FileMetadata) data;
      stat.file = true;
      stat.size = file.getSize();
      stat.time = file.getClientModified().getTime()/1000;
    }
    if(data instanceof FolderMetadata){
      stat.dir = true;
    }
    return stat;
  }

  public Tap<DbxResource> tap() {
    return new DbxTap();
  }

  public Sink<DbxResource> sink() {
    return new DbxSink();
  }

  private class DbxTap extends Tap<DbxResource> {
    protected DbxTap() { super(DbxResource.this); }

    protected Bell start(Bell bell) {
      return new ThreadBell() {
        public Object run() throws Exception {
          /*Issue #25
		  	Modified by: Dhruva Kumar Srinivasa
			Comments: Dropbox v2 API returns a DbxDownloader instance. Call download on the DbxDownloader object, passing an output stream as a parameter*/
          DbxDownloader<FileMetadata> downloader = session.client.files().download(
                  source().path.toString());
          downloader.download(asOutputStream());
          //End changes for Issue #21
          finish();
          return null;
        } public void fail(Throwable t) {
          finish();
        }
      }.startOn(initialize().and(bell));
    }
  }

  private class DbxSink extends Sink<DbxResource> {
    private UploadUploader upload;

    protected DbxSink() { super(DbxResource.this); }

    protected Bell<?> start() {
      return initialize().and((Bell<Stat>)source().stat()).new As<Void>() {
        public Void convert(Stat stat) throws Exception {
          upload = session.client.files().upload(
                  destination().path.toString());
          return null;
        } public void fail(Throwable t) {
          finish(t);
        }
      };
    }

    protected Bell drain(final Slice slice) {
      return new ThreadBell<Void>() {
        public Void run() throws Exception {
          upload.getOutputStream().write(slice.asBytes());
          return null;
        }
      }.start();
    }

    protected void finish(Throwable t) {
      try {
        upload.finish();
      } catch (Exception e) {
        // Ignore...?
      } finally {
        upload.close();
      }
    }
  }
}
