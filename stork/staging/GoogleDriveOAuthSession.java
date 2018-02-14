package stork.staging;

import com.google.api.client.auth.oauth2.AuthorizationCodeRequestUrl;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.DriveScopes;
import stork.core.Config;
import stork.cred.StorkOAuthCred;

import java.util.*;

/** OAuth wrapper for googledrive. */

public class GoogleDriveOAuthSession extends OAuthSession {

    private static String finishURI;
    private static GoogleClientSecrets clientSecrets;
    private static final java.io.File DATA_STORE_DIR = new java.io.File(System.getProperty("user.home"), ".credentials/ods");
    private static final FileDataStoreFactory DATA_STORE_FACTORY;
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    private static final HttpTransport HTTP_TRANSPORT;
    private static final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE_METADATA_READONLY);

    public static class GoogleDriveConfig {
        public String client_id, client_secret, auth_uri, token_uri, auth_provider_x509_cert_url, project_id, redirect_uris;
    }

    static {

        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        GoogleDriveConfig c = Config.global.googledrive;
        List<String> redirect_uris;

        if (c == null || c.client_id == null || c.client_secret == null || c.token_uri == null || c.redirect_uris == null)
            finishURI = null;
        else {
            redirect_uris = Arrays.asList(c.redirect_uris.replaceAll("\\[|\\]|\"|\n","")
                                                        .trim()
                                                        .split(","));
            finishURI = redirect_uris.get(0);

            GoogleClientSecrets.Details details = new GoogleClientSecrets.Details();

            details.setAuthUri(c.auth_uri).setClientId(c.client_id)
                    .setClientSecret(c.client_secret).setRedirectUris(Arrays.asList(finishURI))
                    .setTokenUri(c.token_uri);
            clientSecrets = new GoogleClientSecrets().setInstalled(details);
        }
    }

    private static String getUrl() {
        String url;
        try {
            // Build flow and trigger user authorization request.
            GoogleAuthorizationCodeFlow flow =
                    new GoogleAuthorizationCodeFlow.Builder(
                            HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                            .setDataStoreFactory(DATA_STORE_FACTORY)
                            .build();

            AuthorizationCodeRequestUrl authorizationUrl =
                    flow.newAuthorizationUrl().setRedirectUri(finishURI).setState(flow.getClientId());

            url = authorizationUrl.toURL().toString();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return url;
    }

    public synchronized String start(){
        this.key = clientSecrets.getDetails().getClientId();
        if (finishURI == null)
            throw new RuntimeException("Google Drive config missing");
        return getUrl();
    }

    private static String storeCredential(String code) {
        String accessToken;
        String userId = "user";
        try {
            // Build flow and trigger user authorization request.
            GoogleAuthorizationCodeFlow flow =
                    new GoogleAuthorizationCodeFlow.Builder(
                            HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                            .setDataStoreFactory(DATA_STORE_FACTORY)
                            .build();

            TokenResponse response = flow.newTokenRequest(code).setRedirectUri(finishURI).execute();

            flow.createAndStoreCredential(response, userId);
            accessToken = response.getAccessToken();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return accessToken;
    }

    /** Finish the handshake. */
    public StorkOAuthCred finish(String token){
        String accessToken = storeCredential(token);
        try {
            StorkOAuthCred cred = new StorkOAuthCred(accessToken);
            cred.name = "GoogleDrive";
            return cred;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}