package stork.module.googleDrive;

import stork.feather.*;
import stork.feather.errors.NotFound;
import stork.feather.util.ThreadBell;
//import com.dropbox.core.v2.files.*;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;

import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.*;
import com.google.api.services.drive.Drive;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class GoogleDriveResource extends Resource<GoogleDriveSession, GoogleDriveResource> {

  GoogleDriveResource(GoogleDriveSession session, Path path) {
    super(session, path);
  }

  public synchronized Emitter<String> list() {
    final Emitter<String> emitter = new Emitter<>();
    new ThreadBell() {
      public Object run() throws Exception {
//
//        ListFolderResult listing = session.client.files().listFolderContinue(path.toString());
//        for (Metadata child : listing.getEntries())
//          emitter.emit(child.getName());
//        emitter.ring();
        return null;
      }
    }.startOn(initialize());
    return emitter;
  }

  public synchronized Bell<Stat> stat() {
    return new ThreadBell<Stat>() {
      public Stat run() throws Exception {

//        ListFolderResult data = null;
        Drive.Files.List result = null;

        Stat stat = new Stat();
        stat.name = path.name();
        stat.dir = true;
        if(path.toString() == "/") {

          result = session.service.files().list()
                  .setOrderBy("name")
                  .setQ("trashed=false and 'root' in parents")
                  .setFields("nextPageToken, files(id, name, kind, mimeType, size, modifiedTime)");
        }
        else{
          try {
            String query = new StringBuilder().append("trashed=false and ")
                          .append("'0BzkkzI-oRXwxfjRHVXZxQmhSaldCWWJYX0Y2OVliTkFLbjdzVTBFaWZ5c1RJRF9XSjViQ3c'")
                          .append(" in parents").toString();

            result = session.service.files().list()
                    .setOrderBy("name")
                    .setQ(query)
                    .setFields("nextPageToken, files(id, name, kind, mimeType, size, modifiedTime)");
          }
          catch (Exception e){
              e.printStackTrace();
          }
        }

        if (result == null)
          throw new NotFound();

        FileList fileSet = null;

        if (stat.dir) {
          List<Stat> sub = new LinkedList<>();
          do {
            try {
              fileSet = result.execute();
              List<File> files = fileSet.getFiles();
              for (File file : files) {
                sub.add(mDataToStat(file));
              }
              stat.setFiles(sub);
              result.setPageToken(fileSet.getNextPageToken());
            }
            catch (NullPointerException e) {

            }
            catch (Exception e) {
              fileSet.setNextPageToken(null);
            }
          }
          while (result.getPageToken() != null);
        }
//        if (data == null)
//          stat = mDataToStat(mData);
//        else {
//          stat = mDataToStat(data.getEntries().iterator().next());
//        }
//        stat.name = path.name();

//        if (stat.dir) {
//          ListFolderResult dbd;
//          if(stat.name == "/") {
//            dbd = session.client.files().listFolder("");
//          }
//          else{
//            dbd = session.client.files().listFolder(path.toString());
//          }
//          List<Stat> sub = new LinkedList<>();
//          for (Metadata child : dbd.getEntries())
//            sub.add(mDataToStat(child));
//          stat.setFiles(sub);
//        }
        return stat;
      }
    }.startOn(initialize());
  }

  private Stat mDataToStat(File file) {
    Stat stat = new Stat(file.getName());

    try {
      System.out.println(file.getName());
      stat.file = true;
      stat.id = file.getId();
      stat.time = file.getModifiedTime().getValue()/1000;
      if (file.getMimeType().equals("application/vnd.google-apps.folder")) {
        stat.dir = true;
      }
      else
        stat.size = file.getSize();
    }
    catch (Exception e) {
//          e.printStackTrace();
    }

    return stat;
  }

  public Tap<GoogleDriveResource> tap() {
    return new GoogleDriveTap();
  }

  public Sink<GoogleDriveResource> sink() {
    return new GoogleDriveSink();
  }

  private class GoogleDriveTap extends Tap<GoogleDriveResource> {
    protected GoogleDriveTap() { super(GoogleDriveResource.this); }

    protected Bell start(Bell bell) {
      return new ThreadBell() {
        public Object run() throws Exception {
//          session.client.files().download(
//                  source().path.toString());
          finish();
          return null;
        } public void fail(Throwable t) {
          finish();
        }
      }.startOn(initialize().and(bell));
    }
  }

  private class GoogleDriveSink extends Sink<GoogleDriveResource> {
//    private UploadUploader upload;

    protected GoogleDriveSink() { super(GoogleDriveResource.this); }

    protected Bell<?> start() {
      return initialize().and((Bell<Stat>)source().stat()).new As<Void>() {
        public Void convert(Stat stat) throws Exception {
//          upload = session.client.files().upload(
//                  destination().path.toString());
          return null;
        } public void fail(Throwable t) {
          finish(t);
        }
      };
    }

    protected Bell drain(final Slice slice) {
      return new ThreadBell<Void>() {
        public Void run() throws Exception {
//          upload.getOutputStream().write(slice.asBytes());
          return null;
        }
      }.start();
    }

    protected void finish(Throwable t) {
      try {
//        upload.finish();
      } catch (Exception e) {
        // Ignore...?
      } finally {
//        upload.close();
      }
    }
  }
}
