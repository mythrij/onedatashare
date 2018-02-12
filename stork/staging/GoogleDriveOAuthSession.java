package stork.staging;

import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
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
import org.json.JSONObject;
import stork.core.Config;
import stork.cred.StorkOAuthCred;
import stork.module.googleDrive.GoogleDriveResource;
import sun.net.www.http.HttpClient;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.URL;
import java.util.*;
import java.lang.Object;


/** OAuth wrapper for googledrive. */

public class GoogleDriveOAuthSession extends OAuthSession {

    static String finishURI;
    static String secret;

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
        InputStream in = GoogleDriveOAuthSession.class.getResourceAsStream("client_secret.json");
        GoogleClientSecrets clientSecrets =
                GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow =
                new GoogleAuthorizationCodeFlow.Builder(
                        HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                        .setDataStoreFactory(DATA_STORE_FACTORY)
                        .build();
        LocalServerReceiver.Builder l = new LocalServerReceiver.Builder();
        l.setHost("localhost");
        l.setPort(8080);
        l.setCallbackPath("/api/stork/oauth");
//        l.setLandingPages("http://127.0.0.1:8080/api/stork/oauth", "http://127.0.0.1:8080/api/stork/oauth");

        LocalServerReceiver lsr = l.build();
        com.google.api.client.auth.oauth2.Credential credential
                = new AuthorizationCodeInstalledApp(flow, lsr).authorize("user");
        System.out.println(
                "Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());

        return credential;
    }


    public static class GoogleDriveConfig {
        public String key, secret, redirect;
    };

    {
        GoogleDriveConfig c = Config.global.googledrive;
        if (c != null && c.key != null && c.secret != null && c.redirect != null) {
            this.key = c.key;
            secret = c.secret;
           finishURI = c.redirect;
        } else {
            finishURI = null;
        }
    }

    public synchronized String start(){
//        try {
//            String accessToken = authorize().getAccessToken();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        String url = "https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.metadata.readonly&" +
                "state=25429880102-chquojcll8k4l3hdto2g4olgb315nj0a.apps.googleusercontent.com&" +
                "redirect_uri=http://127.0.0.1:8080/api/stork/oauth&response_type=code&" +
                "client_id=25429880102-chquojcll8k4l3hdto2g4olgb315nj0a.apps.googleusercontent.com";
        return url;
    }

    /** Finish the handshake. */
    public StorkOAuthCred finish(String token){

        Map<String,String[]> map = new HashMap<>();
        map.put("state", new String[] {this.key});
        map.put("code", new String[] {token});
        String accessToken = null;
//        try {
//            accessToken = authorize().getAccessToken();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        accessToken = token;
        try {
            String url = "https://www.googleapis.com/oauth2/v4/token/";
            URL obj = new URL(url);
            HttpsURLConnection con = (HttpsURLConnection) obj.openConnection();

            //add reuqest header
            con.setRequestMethod("POST");
//            con.setRequestProperty("User-Agent", USER_AGENT);
            con.setRequestProperty("Accept-Language", "en-US,en;q=0.5");

            String urlParameters = "code=" +token+"&"+
                    "client_id=" +this.key+"&"+
                    "client_secret=" +secret+"&"+
                    "redirect_uri=" +finishURI+"&"+
                    "grant_type=authorization_code";

            // Send post request
            con.setDoOutput(true);
            DataOutputStream wr = new DataOutputStream(con.getOutputStream());
            wr.writeBytes(urlParameters);
            wr.flush();
            wr.close();

            int responseCode = con.getResponseCode();
            System.out.println("\nSending 'POST' request to URL : " + url);
            System.out.println("Post parameters : " + urlParameters);
            System.out.println("Response Code : " + responseCode);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            System.out.println(response.toString());
            String resp = response.toString();
            JSONObject jsonObj = new JSONObject(resp);
            System.out.println(jsonObj.getString("access_token"));
            accessToken = jsonObj.getString("access_token");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //print result

        try {
           // DbxAuthFinish finish = auth.finish(map);
            StorkOAuthCred cred = new StorkOAuthCred(accessToken);
            cred.name = "GoogleDrive";
            return cred;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
