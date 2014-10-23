package stork.module.smtp;

import stork.feather.*;
import stork.feather.util.*;
import stork.module.*;
import stork.module.ftp.*;

public class SMTPModule extends Module<SMTPResource> {
  {
    name("Stork SMTP Module");
    protocols("mailto");
    description("A module interacting with SMTP systems.");
  }

  public SMTPResource select(URI uri, Credential credential){
    return new SMTPSession(uri).root(); 
  }

  public static void main(String[] argv){
    SMTPSession smtpSession = new SMTPSession(URI.EMPTY);
    Resource smtpResource = smtpSession.root();
    Resource src = new FTPModule().select("ftp://ftp.sunet.se/pub/pictures/animals/deers/deer.gif");
    Transfer tf = src.transferTo(smtpResource);
    tf.start();
    tf.onStop().promise(new Bell() {
        protected void done(){
        System.out.println("Transfer complete");
        }
        protected void fail(Throwable t){
          t.printStackTrace();
        }
    });
  }
}
