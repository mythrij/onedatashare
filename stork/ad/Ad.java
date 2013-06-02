package stork.ad;

import static stork.util.StorkUtil.splitCSV;

import java.util.*;
import java.util.regex.*;
import java.math.BigDecimal;
//import java.nio.*;
import java.io.*;

// TODO: Chained ads break compatibility with ClassAds. Update
// documentation to reflect this. Also remove "LiteAd" references and just
// call them ads. Also remove XML references.
//
// This file defines the stucture and grammar of our key-value store
// utility and language, hereforth referred to as LiteAds, as well as an
// implementation which includes a parser and composer for three different
// representations: LiteAd/Ad, XML, and JSON. LiteAds are intended to
// be a subset of Condor's Ad language -- that is, anything that can
// parse Ads can parse LiteAds.
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
// grammar is assumed to be Java chars. Declaration names are case
// insensitive (internally, all names are lowercased).
//   ads    -> '[' decls ('|' decls)* ']'
//   decls  -> decl (';' decl?)*
//   decl   -> name '=' value
//   name   -> (a-z_) (0-9a-z_)*
//   value  -> number | string | bool | ad
//   number -> [-+]?[0-9]* '.' [0-9]+expo?
//           | [-+]?[0-9]+expo?
//   expo   -> e[-+]?[0-9]+
//   string -> '"' ('\'.|(^"))* '"'
//   bool   -> 'true' | 'false'
//
// The parser can also handle a subset of JSON (everything excluding arrays,
// essentially). The parser determines what format it is parsing based on
// the first character. The syntax for JSON representations of ads is:
//   ad     -> '{' decl (',' decl?)* '}'
//   decl   -> '"' name '"' ':' value
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
// essentially a lighter version of the ClassAd language and is backwards
// compatible.

public class Ad implements Iterable<Ad> {
  // The heart of the structure.
  protected final Map<String, Object> map;
  Ad next = null;
  int mode = AD;

  // Some compiled patterns used for parsing.
  public static final Pattern
    IGNORE = Pattern.compile("(\\s*((#|//).*$)?)+", Pattern.MULTILINE),
    PRINTABLE = Pattern.compile("[\\p{Print}\r\n]*"),
    DECL_ID = Pattern.compile("[a-z_]\\w*", Pattern.CASE_INSENSITIVE);

  public static final int
    AD = 0, JSON = 1, STRING = 1, NUMBER = 2, BOOL = 3;

  public static class ParseError extends RuntimeException {
    public ParseError(String m) {
      super(m);
    }
  }

  // Create a new ad, plain and simple.
  public Ad() {
    this(true, null);
  }

  // Allows subclasses to take maps from input ads directly to prevent
  // needless copying.
  protected Ad(boolean copy, Ad ad) {
    map = (ad == null) ? new LinkedHashMap<String, Object>() :
          (copy) ? new LinkedHashMap<String, Object>(ad.map) : ad.map;
  }

  // Create an ad with given key and value.
  public Ad(String key, int value) {
    this(); put(key, value);
  } public Ad(String key, long value) {
    this(); put(key, value);
  } public Ad(String key, double value) {
    this(); put(key, value);
  } public Ad(String key, boolean value) {
    this(); put(key, value);
  } public Ad(String key, Number value) {
    this(); put(key, value);
  } public Ad(String key, String value) {
    this(); put(key, value);
  } public Ad(String key, Ad value) {
    this(); put(key, value);
  }

  // Merges all of the ads passed into this ad.
  public Ad(Ad... bases) {
    this(); merge(bases);
  }

  // Convenient method for throwing parse errors.
  private static ParseError PE(String m) { return new ParseError(m); }

  // The set of tokens for the parser.
  public static enum Token {
    // Tokens and their patterns. The optional second argument specifies
    // the token in JSON format.
    T_LB ("\\[", "\\{"),
    T_ID ("[\\w\\.]+", "\"[\\w\\.]+\""),
    T_EQ ("=", ":"),
    T_NUM("([-+]?(\\d*\\.\\d+)|(\\d+))(e[-+]?\\d+)?"),
    T_STR("\"[^\\\\\"]*(?:\\\\.[^\\\\\"]*)*?\""),
    T_TF ("(true|false)"),
    T_SLB("\\[", "\\{"),
    T_SC (";", ","),
    T_JP ("\\|"),  // "joiner pipe"
    T_RB ("\\]", "\\}");

    static {
      T_LB.next (T_ID, T_SC, T_JP, T_RB);
      T_JP.next (T_ID, T_SC, T_JP, T_RB);
      T_ID.next (T_EQ);
      T_EQ.next (T_NUM, T_STR, T_TF, T_SLB);
      T_NUM.next(T_SC, T_JP, T_RB);
      T_STR.next(T_SC, T_JP, T_RB);
      T_TF.next (T_SC, T_JP, T_RB);
      T_SLB.next(T_SC, T_JP, T_RB);
      T_SC.next (T_ID, T_JP, T_RB);
    }

    EnumSet<Token> next = null;
    Pattern pattern = null;
    Pattern js_pattern = null;

    void next(Token t1, Token... r) {
      next = EnumSet.of(t1, r);
    }

    // Get the pattern given the mode.
    Pattern pattern(int mode) {
      if (mode == JSON && js_pattern != null)
        return js_pattern;
      return pattern;
    }

    // Constructor
    Token(String re, String jre) {
      pattern = Pattern.compile(re,  Pattern.CASE_INSENSITIVE);
      if (jre != null)
        js_pattern = Pattern.compile(jre, Pattern.CASE_INSENSITIVE);
    } Token(String re) {
      this(re, null);
    }
  }

  // The parser itself. Can be fed bytes/chars incrementally, buffering
  // input, and tossing it as it is processed.
  public static class Parser {
    Ad main_ad;           // The ad we're going to return.
    Ad ad;                // The ad to put declarations into.
    StringBuilder sb;
    Matcher m;
    Token token = null;   // Current token.
    int mode = AD;        // Parser mode. (AD or JSON)
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
      this.ad = main_ad = ad;
    } public Parser(Matcher m) {
      this(new Ad(), m);
    } public Parser(Ad ad, Matcher m) {
      this.m = m;
      this.ad = main_ad = ad;
    } public Parser(Ad ad) {
      this(ad, "");
    } public Parser() {
      this("");
    }

    // Return a string explaining what is expected after a token.
    private static String expectStringFor(Token t, int m) {
      if (t == null) return "beginning of ad: [ or {";
      char a = (m == JSON) ? ':' : '=';
      char s = (m == JSON) ? ',' : ';';
      char e = (m == JSON) ? '}' : ']';
      switch (t) {
        case T_JP :
        case T_LB : return "identifier or terminator";
        case T_ID : return "assignment operator: "+a;
        case T_EQ : return "value (number, string, boolean, or ad)";
        case T_SLB:
        case T_NUM:
        case T_STR:
        case T_TF : return "separator ("+s+") or end of ad: "+e;
        case T_SC : return "identifier or end of ad: "+e;
        case T_RB : return "nothing!";
      } return "unknown";
    }

    // "Chomp" off a piece of the input using the matcher, returning the
    // matched piece, or null if none was found. Adjusts the matcher
    // position if a match was found.
    private synchronized String chomp(Token t, int m) {
      return chomp(t.pattern(m));
    } private synchronized String chomp(Token t) {
      return chomp(t.pattern(mode));
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
      // Hack for T_ID in JSON mode to handle quotes.
      if (mode == JSON && t == Token.T_ID) {
        cid = s.substring(1, s.length()-1).toLowerCase();
      } else switch (t) {
        case T_ID : cid = s.toLowerCase(); break;
        case T_SLB: Parser p = new Parser(m);
                    p.token = Token.T_LB; p.mode = mode;
                    ad.putObject(cid, p.getAd()); break;
        case T_NUM: ad.putObject(cid, new BigDecimal(s)); break;
        case T_STR: s = unescapeString(s.substring(1, s.length()-1));
                    ad.putObject(cid, s); break;
        case T_TF : ad.putObject(cid, Boolean.valueOf(s)); break;
        case T_JP : ad = ad.next(new Ad()); break;
        case T_LB : m.region(m.regionStart()-1, m.regionEnd());
                    Ad ad = Ad.parse(m);
                    // TODO: Check for null here!!
                    ad.putObject(cid, ad);
      } return t;
    }

    // Look for T_LB, and return the character found so we know what parse
    // mode to use.
    private synchronized int findStart() {
      if (chomp(Token.T_LB, AD) != null)
        return AD;
      if (chomp(Token.T_LB, JSON) != null)
        return JSON;
      return -1;
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
        if ((mode = findStart()) >= 0)
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
      } throw PE("expecting "+expectStringFor(token, mode));
    }

    // Return an ad from the parser. Returns null if ad is not yet
    // finished. Throws exception if syntax error occurs.
    private synchronized Ad getAd() {
      while (true) {
        Token t = nextToken();
        if (t == null) 
          return null;
        dws = false;
        if (t.next == null)
          return main_ad.mode(mode);
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
  public synchronized Ad parseInto(InputStream is) {
    try {
      StringBuilder sb = new StringBuilder(1024);
      Parser p = new Parser(this);

      for (int c = is.read(); c >= 0; c = is.read()) {
        sb.append((char)c);
        if (c != ']' && c != '}') continue;
        Ad ad = p.write(sb);
        if (ad != null) return this;
        sb = new StringBuilder(1024);
      } return null;
    } catch (Exception e) {
      throw new RuntimeException("couldn't parse: "+e.getMessage(), e);
    }
  } public static Ad parse(InputStream is) {
    return new Ad().parseInto(is);
  }

  // Parse from a file.
  public synchronized Ad parseInto(File f) {
    try {
      return parseInto(new FileInputStream(f));
    } catch (Exception e) {
      throw new RuntimeException("couldn't parse: "+e.getMessage(), e);
    }
  } public static Ad parse(File f) {
    return new Ad().parseInto(f);
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
      return ((String)o).toLowerCase().equals("true");
    if (o instanceof Number)
      return ((Number)o).intValue() > 0;
    return def;
  }

  // Get an inner ad from this ad. Defaults to null. Should this
  // parse strings?
  public Ad getAd(Object s) {
    return getAd(s, null);
  } public Ad getAd(Object s, Ad def) {
    Object o = getObject(s);
    if (s != null && o instanceof Ad)
      return (Ad)o;
    return def;
  }
    
  // Look up an object by its key. Handles recursive ad lookups.
  protected synchronized Object getObject(Object key) {
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
  private synchronized Ad putObject(Object okey, Object value) {
    int i;
    Ad ad = this;

    if (okey == null)
      throw PE("null key given");
    String key = okey.toString().toLowerCase().trim();
    if (key.isEmpty())
      throw PE("empty key given");

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
      if (!DECL_ID.matcher(key).matches())
        throw PE("invalid key name: "+key);
      if (value != null)
        ad.map.put(key, value);
      else
        ad.map.remove(key);
    } return this;
  }

  // Chaining methods
  // ----------------
  // Insert an ad after this ad in the chain. If null, "cuts" the chain.
  // Returns the inserted ad.
  public synchronized Ad next(Ad ad) {
    next = ad;
    return ad;
  }

  // Get the next ad in the chain.
  public synchronized Ad next() {
    return next;
  }

  // Return whether or not there is another ad in the chain.
  public synchronized boolean hasNext() {
    return next != null;
  }

  // Get an iterator which goes over each ad in the chain.
  public synchronized Iterator<Ad> iterator() {
    return new Iterator<Ad>() {
      Ad ad = Ad.this;
      public boolean hasNext() { return ad != null; }
      public Ad next() { Ad a = ad; ad = a.next; return a; }
      public void remove() { }
    };
  }

  // Other methods
  // -------------
  // Methods to get information about and perform operations on the ad.
  
  // Get the number of fields in this ad.
  public synchronized int size() {
    return map.size();
  }

  // Get the number of sub ads in this ad.
  public int count() {
    int i = 1;
    if (hasNext()) for (Ad a : next) i++;
    return i;
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

  // Get the type of an entry in this ad. Returns -1 if the key is not in
  // the ad or if a catastrophe happened and something weird was found.
  public int typeOf(String key) {
    Object o = getObject(key);
    return (o == null)            ? -1 :
           (o instanceof Ad)      ? AD :
           (o instanceof String)  ? STRING :
           (o instanceof Number)  ? NUMBER :
           (o instanceof Boolean) ? BOOL : -1;
  }

  // Merge ads into this one.
  // XXX Possible race condition? Just don't do crazy stuff like try
  // to insert two ads into each other at the same time.
  // FIXME: Doesn't work quite right for chained ads.
  public synchronized Ad merge(Ad... ads) {
    for (Ad a : ads) if (a != null) synchronized (a) {
      map.putAll(a.map);
    } return this;
  }

  // Return a new ad containing only the given keys. Any required key that
  // is missing will result in an error being thrown.
  public Ad model(String required, String optional) {
    return model(splitCSV(required), splitCSV(optional));
  } public Ad model(String[] required, String[] optional) {
    Ad ad = new Ad();
    for (String s : required) {
      Object o = getObject(s);
      if (o == null)
        throw new RuntimeException("missing required field: "+s);
      ad.putObject(s, o);
    } for (String s : optional) {
      Object o = getObject(s);
      if (o == null)
        continue;
      ad.putObject(s, o);
    } return ad;
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

  // Two ads are equal if they have the same keys and the corresponding
  // values are equal.
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (o instanceof Ad)
      return ((Ad)o).map.equals(this.map);
    return false;
  }

  public int hashCode() {
    return map.hashCode();
  }

  // Composition methods
  // ------------------
  // Methods for presenting and serializing ads.

  // Change the default rendering mode of the ad.
  public Ad mode(int m) {
    if (m == JSON)
      mode = JSON;
    else mode = AD;
    return this;
  }

  // Get the default rendering mode of the ad.
  public int mode() {
    return mode;
  }

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

  // Represent this ad as a nicely-formatted string.
  public synchronized String toString() {
    return (mode == JSON) ?
      AdPrinter.JSON.toString(this) :
      AdPrinter.PRETTY.toString(this);
  }

  // Represent this ad in a compact way for serialization.
  public synchronized byte[] serialize() {
    return serialize(true);
  } public synchronized byte[] serialize(boolean decor) {
    AdPrinter p;
    if (mode == JSON)
      p = (decor) ? AdPrinter.JSON : AdPrinter.BARE_JSON;
    else
      p = (decor) ? AdPrinter.COMPACT : AdPrinter.BARE;
    return p.toBytes(this);
  }

  // Represent this ad as a JSON string.
  public synchronized String toJSON() {
    return AdPrinter.JSON.toString(this);
  }

  public static void main(String args[]) {
    System.out.println("Type an ad:");
    try {
      Ad ad = Ad.parse(System.in);
      System.out.println("Got ad: "+ad);
      System.out.println("    or: "+new String(ad.serialize()));
      System.out.println("    or: "+ad.toJSON());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
