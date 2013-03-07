package stork.util;

import java.util.*;
import java.util.regex.*;
import java.math.BigDecimal;
import java.nio.*;
import java.io.*;

// This file defines the stucture and grammar of our key-value store
// utility and language, hereforth referred to as LiteAds, as well as an
// implementation which includes a parser and composer for three different
// representations: LiteAd/Ad, XML, and JSON. LiteAds are intended to
// be a subset of Condor's Ad language -- that is, anything that can
// parse Ads can parse LiteAds (aside from comments).
//
// Ads contain case-insensitive keys and corresponding values of the
// following five types: number, string, boolean, null, and ad. This means
// LiteAds (like Ads) can be nested. There is no distinction between
// null and undefined values except in cases of representation and set
// operations (e.g., merging).
//
// The parser must be robust enough to handle possibly erroneous input! It
// will be fed user input directly from files and sockets, and should be
// able to withstand any attempts of foulery or badness.
// 
// The syntax of a LiteAd will now be described. The details of encoding
// are assumed to be handled by Java; the data type of characters in the
// grammar is assumed to be Java chars. Alphabetical characters are case
// insensitive (internally, all names are lowercased).
//   ad     -> '[' decl (';' decl?)* ']'
//   decl   -> name '=' value
//   name   -> (a-z_) (0-9a-z_)*
//   value  -> number | string | bool | ad
//   number -> [-+]?[0-9]* '.' [0-9]+expo?
//           | [-+]?[0-9]+expo?
//   expo   -> e[-+]?[0-9]+
//   string -> '"' ('\'.|(^"))* '"'
//   bool   -> 'true' | 'false'
//
// The parser will discard whitespace between tokens (except for whitespace
// within strings, of course). It also will disregard newlines, except in
// the case of comments indicated with a # (in which case it discards
// everything until the end of the line).
//
// Why LiteAds? We originally considered using Condor's Ad library
// for serializable, human-readable communication. However, the Ad
// language has a very rich feature set which makes one-to-one translation
// with JSON and XML somewhat less predicatable, and being able to
// effectively convert between formats easily is something we wanted
// for StorkCloud. Hence, we opted to implement a language that is
// essentially a lighter version of the Ad language and is backwards
// compatible.

public class Ad {
  // The heart of the structure.
  private final Map<String, Object> map;

  // Some compiled patterns used for parsing.
  static final Pattern
    IGNORE = Pattern.compile("(\\s*(#.*$)?)+", Pattern.MULTILINE),
    PRINTABLE = Pattern.compile("[\\p{Print}\r\n]*"),
    DECL_ID = Pattern.compile("[a-z_]\\w*", Pattern.CASE_INSENSITIVE);

  public static class ParseError extends RuntimeException {
    public ParseError(String m) {
      super(m);
    }
  }

  public Ad() {
    map = new LinkedHashMap<String, Object>();
  }

  // Create an ad with given key and value.
  public Ad(Object key, int value) {
    this(); put(key, value);
  }

  public Ad(Object key, long value) {
    this(); put(key, value);
  }

  public Ad(Object key, double value) {
    this(); put(key, value);
  }

  public Ad(Object key, boolean value) {
    this(); put(key, value);
  }

  public Ad(Object key, Number value) {
    this(); put(key, value);
  }

  public Ad(Object key, String value) {
    this(); put(key, value);
  }

  public Ad(Object key, Ad value) {
    this(); put(key, value);
  }

  // Merges all of the ads passed into this ad.
  public Ad(Ad... bases) {
    map = new LinkedHashMap<String, Object>();
    merge(bases);
  }

  // Convenient method for throwing parse errors.
  private static ParseError PE(String m) { return new ParseError(m); }

  // The set of tokens for the parser.
  public static enum Token {
    // Tokens, their patterns, and valid next tokens. (Read bottom up.)
    T_LB ("\\["),
    //T_ID ("[a-z_]\\w*(?:\\.[a-z_]\\w*)*"),
    T_ID ("[\\w\\.]+"),
    T_EQ ("="),
    T_NUM("([-+]?(\\d*\\.\\d+)|(\\d+))(e[-+]?\\d+)?"),
    T_STR("\"[^\\\\\"]*(?:\\\\.[^\\\\\"]*)*?\""),
    T_TF ("(true|false)"),
    T_SLB("\\["),
    T_SC (";"),
    T_RB ("\\]");

    static {
      T_LB.next (T_ID, T_SC, T_RB);
      T_ID.next (T_EQ);
      T_EQ.next (T_NUM, T_STR, T_TF, T_SLB);
      T_NUM.next(T_SC, T_RB);
      T_STR.next(T_SC, T_RB);
      T_TF.next (T_SC, T_RB);
      T_SLB.next(T_SC, T_RB);
      T_SC.next (T_ID, T_RB);
    }

    EnumSet<Token> next = null;
    Pattern pattern = null;

    void next(Token t1, Token... r) {
      next = EnumSet.of(t1, r);
    }

    // Constructor
    Token(String re) {
      pattern = Pattern.compile(re, Pattern.CASE_INSENSITIVE);
    }
  }

  // The parser itself. Can be fed bytes/chars incrementally, buffering
  // input, and tossing it as it is processed.
  public static class Parser {
    StringBuilder sb;
    Matcher m;
    Token token = null;   // Current token.
    Ad ad;                // The ad currently being written to.
    Parser sub = null;    // The sub-parser for recursive ad parsing.
    String cid = null;    // Current identifier we're parsing.
    boolean dws = false;  // True if we discarded whitespace already.
    boolean hitEnd = false, requireEnd = false;

    public Parser(CharSequence c) {
      this(new Ad(), c);
    } public Parser(Ad ad, CharSequence c) {
      sb = new StringBuilder();
      sb.append(c.toString());
      m = IGNORE.matcher(sb);
      this.ad = ad;
    } public Parser(Matcher m) {
      this(new Ad(), m);
    } public Parser(Ad ad, Matcher m) {
      this.m = m;
      this.ad = ad;
    } public Parser(Ad ad) {
      this(ad, "");
    } public Parser() {
      this("");
    }

    // Return a string explaining what is expected after a token.
    private static String expectStringFor(Token t) {
      if (t == null) return "beginning of ad: [";
      switch (t) {
        case T_LB : return "identifier (must start with letter)";
        case T_ID : return "assignment operator: =";
        case T_EQ : return "value (number, string, boolean, or ad)";
        case T_SLB:
        case T_NUM:
        case T_STR:
        case T_TF : return "separator (;) or end of ad: ]";
        case T_SC : return "identifier or end of ad: ]";
        case T_RB : return "nothing!";
      } throw new Error("unknown token: "+t);
    }

    // "Chomp" off a piece of the input using the matcher, returning the
    // matched piece, or null if none was found. Adjusts the matcher
    // position if a match was found.
    private synchronized String chomp(Token t) {
      //System.out.println("Chomping: "+t);
      return chomp(t.pattern);
    } private synchronized String chomp(Pattern p) {
      m.usePattern(p);
      if (!m.lookingAt()) return null;
      String g = m.group();
      hitEnd = m.hitEnd();
      requireEnd = m.requireEnd();
      m.region(m.end(), m.regionEnd());
      return g;
    }

    // Handle parsing a token and potentially changing the ad.
    private synchronized Token handleToken(Token t, String s) {
      //System.out.println("handling: "+t+", "+s);
      switch (t) {
        case T_ID : cid = s; break;
        case T_SLB: m.region(m.regionStart()-1, m.regionEnd());
                    ad.putObject(cid, Ad.parse(m)); break;
        case T_NUM: ad.putObject(cid, new BigDecimal(s)); break;
        case T_STR: s = unescapeString(s.substring(1, s.length()-1));
                    ad.putObject(cid, s); break;
        case T_TF : ad.putObject(cid, new Boolean(s)); break;
        case T_LB : m.region(m.regionStart()-1, m.regionEnd());
                    ad.putObject(cid, Ad.parse(m));
      } return t;
    }

    // Check the sequence for the the next token and return it.
    private synchronized Token nextToken() {
      // See if we need to discard ignored space first. If we hit
      // the end, we need more input to do anything.
      while (!dws) {
        String s = chomp(IGNORE);
        //System.out.println("Tossed: "+s.length());
        if (m.hitEnd()) return null;
        if (s == null || s.isEmpty()) dws = true;
      }

      if (token == null) {
        // If no pattern set, look for the opening bracket.
        if (chomp(Token.T_LB) != null)
          return token = Token.T_LB;
      } else if (token.next == null) {
        Token t = token;
        token = null;
        return t;
      } else for (Token t : token.next) {
        // Otherwise look for the next token.
        String s = chomp(t);
        //System.out.println("Token: "+s);
        //System.out.println("region: "+m.regionStart()+" "+m.regionEnd());
        if (requireEnd) return null;
        if (s != null) return token = handleToken(t, s);
      } throw PE("expecting "+expectStringFor(token));
    }

    // Return an ad from the parser. Returns null if ad is not yet
    // finished. Throws exception if syntax error occurs.
    private synchronized Ad getAd() {
      while (true) {
        Token t = nextToken();
        if (t == null) return null;
        dws = false;
        if (t.next == null) return ad;
      }
    }

    // Buffer some characters and attempt to parse the whole buffer
    // as an ad. Throws a ParseError if the data is not a valid ad,
    // otherwise returns null to indicate more information is needed.
    // XXX This isn't very good.
    public synchronized Ad write(CharSequence b) {
      sb.append(b);
      m.region(m.regionStart(), sb.length());
      Ad ad;

      try {
        ad = getAd();
        //System.out.println("Ad is: "+ad);
      } catch (ParseError e) {
        if (hitEnd)
          return null;
        throw e;
      }

      if (ad != null)
        sb.delete(m.regionEnd(), sb.length());
      m.reset();
      token = null;

      return ad;
    }
  }

  // Parse an ad using a matcher.
  public synchronized Ad parseInto(Matcher m) {
    Parser p = new Parser(this, m);
    return p.getAd();
  } public static Ad parse(Matcher m) {
    return new Ad().parseInto(m);
  }

  // Parse a character sequence.
  public synchronized Ad parseInto(CharSequence cs) {
    Parser p = new Parser(this, cs);
    return p.getAd();
  } public static Ad parse(CharSequence cs) {
    return new Ad().parseInto(cs);
  }

  static Pattern AD_END = Pattern.compile("(?<=\\])");

  // Parse an input stream into this ad. XXX This is hacky and bad.
  public synchronized Ad parseInto(InputStream is) throws IOException {
    StringBuilder sb = new StringBuilder(1024);
    Parser p = new Parser(this);

    for (int c = is.read(); c >= 0; c = is.read()) {
      sb.append((char)c);
      if (c != ']') continue;
      Ad ad = p.write(sb);
      if (ad != null) return this;
      sb = new StringBuilder(1024);
    } return null;
  } public static Ad parse(InputStream is) throws IOException {
    return new Ad().parseInto(is);
  }

  // Access methods
  // --------------
  // Each accessor can optionally have a default value passed as a second
  // argument to be returned if no entry with the given name exists.
  // All of these eventually synchronize on getObject.

  // Get an entry from the ad as a string. Default: null
  public String get(Object s) {
    return get(s, null);
  } public String get(Object s, String def) {
    Object o = getObject(s);
    return (o != null) ? o.toString() : def;
  }

  // Get an entry from the ad as an integer. Defaults to -1.
  public int getInt(Object s) {
    return getInt(s, -1);
  } public int getInt(Object s, int def) {
    Number n = getNumber(s);
    return (n != null) ? n.intValue() : def;
  }

  // Get an entry from the ad as a long. Defaults to -1.
  public long getLong(Object s) {
    return getLong(s, -1);
  } public long getLong(Object s, long def) {
    Number n = getNumber(s);
    return (n != null) ? n.longValue() : def;
  }

  // Get an entry from the ad as a double. Defaults to -1.
  public double getDouble(Object s) {
    return getDouble(s, -1);
  } public double getDouble(Object s, double def) {
    Number n = getNumber(s);
    return (n != null) ? n.doubleValue() : def;
  }

  // Get an entry from the ad as a Number object. Attempts to cast to a
  // number object if it's a string. Defaults to null.
  public Number getNumber(Object s) {
    return getNumber(s, null);
  } public Number getNumber(Object s, Number def) {
    Object o = getObject(s);
    if (s != null) try {
      if (o instanceof Number)
        return (Number) o;
      if (o instanceof String)
        return new BigDecimal((String)o);
    } catch (Exception e) {
      // Parse error, fall through.
    } return def;
  }

  // Get an entry from the ad as a boolean. Returns true if the value is
  // a true boolean, a string equal to "true", or a number other than zero.
  // Returns def if key is an ad or is undefined. Defaults to false.
  public boolean getBoolean(Object s) {
    return getBoolean(s, false);
  } public boolean getBoolean(Object s, boolean def) {
    Object o = getObject(s);
    if (o instanceof Boolean)
      return ((Boolean)o).booleanValue();
    if (o instanceof String)
      return Boolean.getBoolean((String)o);
    if (o instanceof Number)
      return ((Number)o).intValue() != 0;
    return def;
  }

  // Get an inner ad from this ad. Defaults to null. Should this parse
  // strings?
  public Ad getAd(Object s) {
    return getAd(s, null);
  } public Ad getAd(Object s, Ad def) {
    Object o = getObject(s);
    return (s != null && o instanceof Ad) ? (Ad)o : def;
  }
    
  // Look up an object by its key. Handles recursive ad lookups.
  private synchronized Object getObject(Object key) {
    if (key == null)
      throw PE("null key given");
    return getObject(key.toString().toLowerCase());
  } private synchronized Object getObject(String key) {
    // XXX Call only after sanity checks.
    int i;
    Ad ad = this;

    // Keep traversing ads until we find ad we need.
    while ((i = key.indexOf('.')) > 0) synchronized (ad) {
      String k1 = key.substring(0, i);
      Object o = ad.map.get(k1);
      if (o instanceof Ad)
        ad = (Ad) o;
      else return null;
      key = key.substring(i+1);
    }

    // No more ads to traverse, get value.
    synchronized (ad) {
      return ad.map.get(key);
    }
  }

  // Insertion methods
  // -----------------
  // Methods for putting values into an ad. Certain primitive types can
  // be stored as their wrapped equivalents to save space, since they are
  // still printed in a way that is compatible with the language.
  // All of these eventually synchronize on putObject.
  public Ad put(Object key, int value) {
    return putObject(key, Integer.valueOf(value));
  }

  public Ad put(Object key, long value) {
    return putObject(key, Long.valueOf(value));
  }

  public Ad put(Object key, double value) {
    return putObject(key, Double.valueOf(value));
  }

  public Ad put(Object key, Boolean value) {
    return putObject(key, value);
  }

  public Ad put(Object key, boolean value) {
    return putObject(key, Boolean.valueOf(value));
  }

  public Ad put(Object key, Number value) {
    return putObject(key, value);
  }

  public Ad put(Object key, String value) {
    if (value != null && !PRINTABLE.matcher(value).matches())
      throw PE("non-printable characters in string");
    return putObject(key, value);
  }

  // If you create a loop with this and print it, I feel bad for you. :)
  public Ad put(Object key, Ad value) {
    return putObject(key, value);
  }

  // Use this to insert objects in the above methods. This takes care
  // of validating the key so accidental badness doesn't occur.
  private synchronized Ad putObject(Object key, Object value) {
    if (key == null)
      throw PE("null key given");
    String k = key.toString().toLowerCase().trim();
    if (k.isEmpty())
      throw PE("empty key given");
    return putObject(key.toString().toLowerCase(), value);
  } private synchronized Ad putObject(String key, Object value) {
    int i;
    Ad ad = this;

    // Keep traversing ads until we find ad we need to insert into.
    while ((i = key.indexOf('.')) > 0) synchronized (ad) {
      String k1 = key.substring(0, i);
      if (!DECL_ID.matcher(k1).matches())
        throw PE("invalid key name: "+key);
      Object o = ad.map.get(k1);
      if (o == null)
        ad.map.put(k1, ad = new Ad());
      else if (o instanceof Ad)
        ad = (Ad) o;
      else
        throw PE("key path contains non-ad");
      key = key.substring(i+1);
    }

    // No more ads to traverse, insert key.
    synchronized (ad) {
      if (value != null)
        ad.map.put(key, value);
      else
        ad.map.remove(key);
    } return this;
  }

  // Other methods
  // -------------
  // Methods to get information about and perform operations on the ad.
  
  // Get the number of fields in this ad.
  public synchronized int size() {
    return map.size();
  }

  // Check if fields are present in the ad.
  public synchronized boolean has(String... keys) {
    return require(keys) == null;
  }

  // Ensure that all fields are present in the ad. Returns the first
  // string which doesn't exist in the ad, or null if all are found.
  public synchronized String require(String... keys) {
    for (String k : keys) if (getObject(k) == null)
      return k;
    return null;
  }

  // Merge ads into this one.
  // XXX Possible race condition? Just don't do crazy stuff like try
  // to insert two ads into each other at the same time.
  public synchronized Ad merge(Ad... ads) {
    for (Ad a : ads) synchronized (a) {
      map.putAll(a.map);
    } return this;
  }

  // Remove fields from this ad.
  public synchronized Ad remove(String... keys) {
    for (String k : keys)
      removeKey(k);
    return this;
  }

  // Return a new ad containing only the specified keys.
  // How should this handle sub ads?
  public synchronized Ad filter(String... keys) {
    Ad a = new Ad();
    for (String k : keys)
      a.putObject(k, getObject(k));
    return a;
  }

  // Rename a key in the ad. Does nothing if the key doesn't exist.
  public synchronized Ad rename(String o, String n) {
    Object obj = removeKey(o);
    if (obj != null)
      putObject(n, obj);
    return this;
  }

  // Trim strings in this ad, removing empty strings.
  public synchronized Ad trim() {
    Iterator<Map.Entry<String,Object>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String,Object> e = it.next();
      Object o = e.getValue();

      if (o instanceof String) {
        String s = o.toString().trim();
        if (s.isEmpty())
          it.remove();
        else e.setValue(s);
      } else if (o instanceof Ad) ((Ad)o).trim();
    } return this;
  }

  // Remove a key from the ad, returning the old value (or null).
  private synchronized Object removeKey(String key) {
    int i;
    Ad ad = this;

    // Keep traversing ads until we find ad we need to remove from.
    while ((i = key.indexOf('.')) > 0) synchronized (ad) {
      String k1 = key.substring(0, i);
      Object o = ad.map.get(k1);
      if (o instanceof Ad)
        ad = (Ad) o;
      else return this;
      key = key.substring(i+1);
    }

    // No more ads to traverse, remove key.
    synchronized (ad) {
      return ad.map.remove(key);
    }
  }

  // Composition methods
  // ------------------
  // Methods for presenting and serializing ads.

  // Reinventing wheels because replace() doesn't work for this. Translates
  // an escaped string parsed from a text ad into a proper Java string.
  private static String unescapeString(String s) {
    boolean esc = false;
    StringBuilder sb = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '\n' || c == '\r') {  // Ignore newlines.
        continue;
      } if (esc) switch (c) {
        case '"' :
        case '\\': sb.append(c);    break;
        case 'n' : sb.append('\n'); break;
        default  : sb.append('\\').append(c);
      } else if (c == '\\') {
        esc = true;
        continue;
      } else {
        sb.append(c);
      } esc = false;
    } return sb.toString();
  }

  // Translate a Java string into an escaped string for presentation.
  private static String escapeString(String s) {
    StringBuilder sb = new StringBuilder(s.length()+10);
    char c;
    for (int i = 0; i < s.length(); i++) switch (c = s.charAt(i)) {
      case '\n': sb.append("\\n");  break;
      case '\\': sb.append("\\\\"); break;
      case '"' : sb.append("\\\""); break;
      default  : sb.append(c);
    } return sb.toString();
  } 

  // Represent this ad as a nicely-formatted string.
  public synchronized String toString() {
    return toString(true);
  }

  // Represent this ad in a compact way for serialization.
  public synchronized byte[] serialize() {
    return toString(false).getBytes();
  }

  // TODO: Make this a little prettier maybe?
  public synchronized String toString(boolean pretty) {
    StringBuffer sb = new StringBuffer();
    String s = !pretty ? "" : (map.size() > 1) ? "\n" : " ";  // separator
    String i = (pretty && map.size() > 1) ? "  " : "";  // indentation
    Iterator<Map.Entry<String,Object>> it = map.entrySet().iterator();
    sb.append("["+s);
    while (it.hasNext()) {
      Map.Entry<String,Object> e = it.next();
      sb.append(i+e.getKey()+stringify(e.getValue(), pretty));
      if (it.hasNext()) sb.append(';');
      if (pretty) sb.append(s);
    }
    sb.append("]");
    return sb.toString();
  }

  // Turns entries into strings suitable for printing/serializing.
  private static String stringify(Object v, boolean p) {
    String q = !p ? "=" : " = ";  // assignment sign
    if (v instanceof String) {  // We need to escape the string.
      String s = escapeString(v.toString());
      return q+'"'+s+'"';
    } if (v instanceof Ad) {  // We need to indent (if pretty printing).
      Ad a = (Ad)v;
      if (a.size() == 1) for (String k : a.map.keySet()) {
        Object o = a.map.get(k);
        if (o instanceof Ad)
          return '.'+k+stringify(o, p);
        return '.'+k+stringify(a.map.get(k), p);
      }
      String s = a.toString(p);
      return q+(p ? s.replace("\n", "\n  ") : s);
    } return q+v.toString();
  }

  // Represent this ad as a JSON string.
  public synchronized String toJSON() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    Iterator<Map.Entry<String,Object>> it = map.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String,Object> e = it.next();
      Object o = e.getValue();
      sb.append('"'+e.getKey()+"\":");
      if (o instanceof Ad)
        sb.append(((Ad)o).toJSON());
      else if (o instanceof String)
        sb.append('"'+escapeString(o.toString())+'"');
      else
        sb.append(o.toString());
      if (it.hasNext()) sb.append(',');
    }
    sb.append("}");
    return sb.toString();
  }

  public static void main(String args[]) {
    System.out.println("Type an ad:");
    try {
      Ad ad = Ad.parse(System.in);
      System.out.println("a = \""+ad.get("a")+"\"");
      System.out.println("Got ad: "+ad);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
