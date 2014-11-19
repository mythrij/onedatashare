package stork.module.http;

import org.jsoup.*;
import org.jsoup.nodes.*;
import org.jsoup.select.*;

import stork.feather.*;
import stork.feather.util.*;

/** A wrapper around Jsoup which can extract links. */
public class HTMLParser extends Emitter<String> {
  private final String uri;

  /** Construct an HTMLParser that will parse from {@code uri}. */
  public HTMLParser(URI uri) {
    this.uri = uri.toString();

    new ThreadBell<Document>() {
      public Document run() throws Exception {
        return Jsoup.connect(HTMLParser.this.uri).get();
      } public void done(Document doc) {
        extract("abs:href", doc.select("a[href]"));
        extract("abs:src", doc.select("[src]"));
        HTMLParser.this.ring();
      } public void fail(Throwable t) {
        HTMLParser.this.ring(t);
      }
    }.start();
  }

  /** Extract and emit relevent elements. */
  private void extract(String attr, Elements elements) {
    for (Element e : elements) try {
      String sub = URI.create(e.attr(attr)).toString();
      if (!sub.startsWith(uri))
        continue;

      sub = sub.substring(uri.length());
      sub = sub.split("#")[0];
      sub = sub.split("?")[0];
      sub = sub.split("/")[0];

      if (!sub.isEmpty())
        emit(sub);
    } catch (Exception ex) {
      // Bad URI?
    }
  }
}
