package stork.core.server;

import java.util.*;
import javax.mail.*;
import javax.mail.internet.*;
import javax.activation.*;

import stork.core.*;
import stork.feather.Bell;
import stork.feather.util.*;
import stork.util.*;

/** A class for composing and sending email. */
public class Mail {
  public String from, to, subject, body;

  /** Asynchronously send the message. */
  public Bell<?> send() {
    return new ThreadBell<Void>() {
      public Void run() {
        Mail.this.run();
        return null;
      }
    }.start();
  }

  /** Send the message. */
  public void run() {
    String smtp = Config.global.smtp_server;
    if (smtp == null)
      throw new Error("SMTP is not configured.");

    Properties prop = System.getProperties();
    Session session = Session.getDefaultInstance(prop);
    prop.setProperty("mail.smtp.host", smtp);

    try {
      MimeMessage msg = new MimeMessage(session);
      msg.setFrom(new InternetAddress(from));
      msg.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
      msg.setSubject(subject);
      msg.setText(body);

      Log.info("Sending mail to: ", to);
      Transport.send(msg);
      Log.info("Mail send successfuly to: ", to);
    } catch (Exception e) {
      Log.info("Failed sending mail to: ", to);
      throw new RuntimeException(e);
    }
  }
}
