package stork.module.googleDrive;

import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.java6.auth.oauth2.VerificationCodeReceiver;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import stork.cred.StorkOAuthCred;
import stork.feather.*;
import stork.feather.errors.AuthenticationRequired;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;


public class GoogleDriveSession extends Session<GoogleDriveSession, GoogleDriveResource> {

  Drive service;

  public GoogleDriveSession(URI uri, Credential cred) {
    super(uri, cred);
  }

  public GoogleDriveResource select(Path path) {
    return new GoogleDriveResource(this, path);
  }

  static String APPLICATION_NAME = "Drive API Java Quickstart";
  static java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"), ".credentials/drive-java-quickstart");
  static FileDataStoreFactory DATA_STORE_FACTORY;
  static JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  static HttpTransport HTTP_TRANSPORT;
  static List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE_METADATA_READONLY);

  public static void initGoogle() {
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }
  }

  public static com.google.api.client.auth.oauth2.Credential authorize() throws IOException {
    // Load client secrets.
    initGoogle();
    InputStream in =
            GoogleDriveSession.class.getResourceAsStream("client_secret.json");
    GoogleClientSecrets clientSecrets =
            GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

    // Build flow and trigger user authorization request.
    GoogleAuthorizationCodeFlow flow =
            new GoogleAuthorizationCodeFlow.Builder(
                    HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                    .setDataStoreFactory(DATA_STORE_FACTORY)
                    .build();

    com.google.api.client.auth.oauth2.Credential credential = new AuthorizationCodeInstalledApp(
            flow, new LocalServerReceiver()).authorize("user");
    System.out.println(
            "Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());

    return credential;
  }

  public static Drive getDriveService() throws IOException {
    com.google.api.client.auth.oauth2.Credential credential = authorize();
    return new Drive.Builder(
            HTTP_TRANSPORT, JSON_FACTORY, credential)
            .setApplicationName(APPLICATION_NAME)
            .build();
  }

  public Bell<GoogleDriveSession> initialize() {
    // If an OAuth token is provided, use it.
    if (credential instanceof StorkOAuthCred) {
//      StorkOAuthCred oauth = (StorkOAuthCred) credential;
//      DbxRequestConfig config = DbxRequestConfig.newBuilder("StorkCloud").build();
      try {
        service = getDriveService();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return Bell.wrap(this);
    }

    throw new AuthenticationRequired("oauth");
  }
}
