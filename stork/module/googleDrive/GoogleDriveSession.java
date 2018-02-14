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
import stork.core.Config;
import stork.cred.StorkOAuthCred;
import stork.feather.*;
import stork.feather.errors.AuthenticationRequired;
import stork.staging.GoogleDriveOAuthSession;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;


public class GoogleDriveSession extends Session<GoogleDriveSession, GoogleDriveResource> {

  static GoogleClientSecrets clientSecrets;
  Drive service;

  public GoogleDriveSession(URI uri, Credential cred) {
    super(uri, cred);
  }

  public GoogleDriveResource select(Path path) {
    return new GoogleDriveResource(this, path);
  }

  static String APPLICATION_NAME = "OneDataShare";
  private static final java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"), ".credentials/ods");
  private static FileDataStoreFactory DATA_STORE_FACTORY;
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static HttpTransport HTTP_TRANSPORT;
  private static final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE_METADATA_READONLY);

  public static void initGoogle() {
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }

    GoogleDriveOAuthSession.GoogleDriveConfig c = Config.global.googledrive;
    List<String> redirect_uris;

    if (c != null && c.client_id != null && c.client_secret != null && c.redirect_uris != null) {
      redirect_uris = Arrays.asList(c.redirect_uris.replaceAll("\\[|\\]|\"|\n","")
              .trim()
              .split(","));
      String finishURI = redirect_uris.get(0);

      GoogleClientSecrets.Details details = new GoogleClientSecrets.Details();

      details.setAuthUri(c.auth_uri).setClientId(c.client_id)
              .setClientSecret(c.client_secret).setRedirectUris(Arrays.asList(finishURI))
              .setTokenUri(c.token_uri);
      clientSecrets = new GoogleClientSecrets().setInstalled(details);

    }
  }

  public static com.google.api.client.auth.oauth2.Credential authorize() throws IOException {
    // Load client secrets.
    initGoogle();

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