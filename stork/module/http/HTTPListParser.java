package stork.module.http;

import java.io.*;
import java.util.*;

import org.jsoup.*;
import org.jsoup.nodes.*;

import stork.feather.*;
import stork.feather.util.*;

/**
 * Extracts links from HTML pages.
 */
public class HTTPListParser {
  private final InputStream is;
  private final URI base;
  private Bell<Set<Stat>> listBell;
  /** Names of files directly under base. */
  private Set<Stat> stats = new HashSet<Stat>();

  /** Extract a listing from {@code source}. This starts {@code source}. */
  public HTTPListParser(URI base, Tap source) {
    this.base = base;
    is = Pipes.asInputStream(source);
    source.start();
  }

  /** Parse and return the listing. */
  public synchronized Bell<Set<Stat>> getListing() {
    if (listBell == null) listBell = new ThreadBell<Set<Stat>>() {
      public Set<Stat> run() throws Exception {
        Document doc = Jsoup.parse(is, "UTF-8", base.toString());

        for (Element e : doc.select("a[href]")) {
          addName(e.attr("href"));
        } for (Element e : doc.select("[src]")) {
          addName(e.attr("src"));
        } for (Element e : doc.select("link[href]")) {
          addName(e.attr("href"));
        }
        return stats;
      }
    }.start();
    return listBell.detach();
  }

  /** Add a relative path. */
  private void addName(String name) {
    URI uri = URI.create(name);
    if (uri.isAbsolute())
      return;
    Path path = base.path().appendLiteral(name);
    if (!path.isRoot() && path.up().equals(base.path()))
      stats.add(createStat(path.name()));
  }

  /** Create a Stat from a name. */
  private Stat createStat(String name) {
    boolean dir = name.endsWith("/");
    if (dir)
      name = name.replaceAll("/+$", "");
    Stat stat = new Stat(name);
    stat.dir = dir;
    stat.file = !dir;
    return stat;
  }
}
