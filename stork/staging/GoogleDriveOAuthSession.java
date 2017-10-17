package stork.staging;

import org.json.JSONObject;
import stork.core.Config;
import stork.cred.StorkOAuthCred;
import sun.net.www.http.HttpClient;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.lang.Object;


/** OAuth wrapper for googledrive. */

public class GoogleDriveOAuthSession extends OAuthSession {

    static String finishURI;
    static String secret;

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
        String url = "https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.metadata.readonly&" +
                "state=25429880102-chquojcll8k4l3hdto2g4olgb315nj0a.apps.googleusercontent.com&" +
                "redirect_uri=http://localhost:8080/api/stork/oauth&response_type=code&" +
                "client_id=25429880102-chquojcll8k4l3hdto2g4olgb315nj0a.apps.googleusercontent.com";
        return url;
    }

    /** Finish the handshake. */
    public StorkOAuthCred finish(String token){

        Map<String,String[]> map = new HashMap<String,String[]>();
        map.put("state", new String[] {this.key});
        map.put("code", new String[] {token});

        String accessToken = token;

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
           // Player ronaldo = new ObjectMapper().readValue(jsonString, Player.class);
            accessToken = jsonObj.getString("access_token");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        //print result

        try {
           // DbxAuthFinish finish = auth.finish(map);
            StorkOAuthCred cred = new StorkOAuthCred(accessToken);
            cred.name = "googledrive";
            return cred;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
