package stork.util;

import java.util.*;
import java.util.regex.*;
import java.math.BigDecimal;

// This file defines the stucture and grammar of our key-value store
// utility and language, hereforth referred to as LiteAds, as well as an
// implementation which includes a parser and composer for three different
// representations: LiteAd/ClassAd, XML, and JSON. LiteAds are intended to
// be a subset of Condor's ClassAd language -- that is, anything that can
// parse ClassAds can parse LiteAds (aside from comments).
//
// Ads contain case-insensitive keys and corresponding values of the
// following five types: number, string, boolean, null, and ad. This means
// LiteAds (like ClassAds) can be nested. There is no distinction between
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
//   number -> [-+]?[0-9]* '.' [0-9]+ expo?
//           | [-+]?[0-9]+ expo?
//   expo   -> e[-+]?[0-9]+
//   string -> '"' ('\'.|(^"))* '"'
//   bool   -> 'true' | 'false'
//
// The parser will discard whitespace between tokens (except for whitespace
// within strings, of course). It also will disregard newlines, except in
// the case of comments indicated with a # (in which case it discards
// everything until the end of the line).
//
// Why LiteAds? We originally considered using Condor's ClassAd library
// for serializable, human-readable communication. However, the ClassAd
// language has a very rich feature set which makes one-to-one translation
// with JSON and XML somewhat less predicatable, and being able to
// effectively convert between formats easyily is something we wanted
// for StorkCloud. Hence, we opted to implement a language that is
// essentially a lighter version of the ClassAd language and is backwards
// compatible.

public class Ad {
  // The heart of the structure. Praise be to Java.
  private final Map<String, Object> map;

  // Some compiled patterns used for parsing.
  private static final int pf = Pattern.CASE_INSENSITIVE;
  private static final Pattern
    IGNORE    = Pattern.compile("(?:\\s*(?:#.*$)?)*"),
    AD_START  = Pattern.compile("[\\[]"),
    PRINTABLE = Pattern.compile("\\p{Print}*"),
    DECL_NAME = Pattern.compile("[a-z_]\\w*", pf),
    DECL_EQ   = Pattern.compile("="),
    DECL_VAL1 = Pattern.compile("[0-9-+\"tfn\\[]", pf),
    DECL_END  = Pattern.compile("[\\];]"),
    P_NUMBER  = Pattern.compile(
      "(([-+]?[0-9]*\\.[0-9]+)|([-+]?[0-9]+))(e[-+]?[0-9]+)?", pf),
    P_STRING  = Pattern.compile("\"(((\\\")|[^\"])*)\""),
    P_BOOL    = Pattern.compile("(true)|(false)", pf);

  public static class ParseError extends Error {
    public ParseError(Object m) {
      super(m.toString());
    }
  }

  public Ad() {
    map = new LinkedHashMap<String, Object>();
  }

  // Merges all of the ads passed into this ad.
  public Ad(Ad... bases) {
    map = new LinkedHashMap<String, Object>();
    mergeInto(bases);
  }

  // Convenient method for throwing parse errors.
  private static ParseError PE(Object m) { return new ParseError(m); }

  // Convenient method for finding a token in a matcher and adjusting the
  // matcher state. Takes care of discarding whitespace and comments.
  // Returns the wth group from the match.
  private static String findToken(Matcher m, Pattern p, int w) {
    if (p != IGNORE) findToken(m, IGNORE, 0);  // Toss ignored crap.
    m.usePattern(p);
    if (!m.lookingAt())
      return null;
    String s = m.group(w);
    m.region(m.end(), m.regionEnd());
    return s;
  }

  // Parse an ad given a matcher (used for recursive ads and multi-ad
  // sequences). This modifies the state of the matcher. If the ad is
  // parsed sucessfully, the region for the matcher will be updated to
  // no longer include the matched ad. Returns null if the end was
  // reached without encountering a parse error or an ad.
  public synchronized Ad parseInto(Matcher m) {
    String t;

    // Read all the whitespace we can and get the first character.
    findToken(m, IGNORE, 0);
    if (m.hitEnd()) return null;

    // Check that we're at the beginning of an ad.
    if (findToken(m, AD_START, 0) == null)
      throw PE("start of ad not found");

    // Start reading declarations in a loop.
    while (true) {
      // Get the name of the declaration.
      if ((t = findToken(m, DECL_NAME, 0)) == null)
        throw PE("expecting variable name");
      String name = t.toLowerCase();

      // Look for the all-important equals sign.
      if (findToken(m, DECL_EQ, 0) == null)
        throw PE("expecting = sign after name");

      // Get the first character of the value, then switch.
      if ((t = findToken(m, DECL_VAL1, 0)) == null)
        throw PE("could not determine value type, check: "+name);
      m.region(m.regionStart()-1, m.regionEnd());  // Faking lookahead...
      switch (t.toLowerCase().charAt(0)) {
        case '"':
          t = findToken(m, P_STRING, 1);
          System.out.println("String: "+t);
          if (t == null)
            throw PE("unterminated string, check: "+name);
          if (!PRINTABLE.matcher(t).matches())
            throw PE("non-printable characters in string, check: "+name);
          t = t.replace("\\\\", "\\").replace("\\\"", "\"");
          map.put(name, t);
          break;
        case 't':
        case 'f':
          t = findToken(m, P_BOOL, 0);
          if (t == null)
            throw PE("unexpected symbol, check: "+name);
          map.put(name, Boolean.valueOf(t)); break;
        case '[':  // Ooh, getting recursive on us huh?
          map.put(name, new Ad().parseInto(m)); break;
        default:  // By process of elimintation, it's a number!
          t = findToken(m, P_NUMBER, 0);
          if (t == null)
            throw PE("unable to parse number, check: "+name);
          map.put(name, new BigDecimal(t));
      }

      // Now check for the end of the ad.
      if ((t = findToken(m, DECL_END, 0)) == null)
        throw PE("expecting ; or ]");
      do {
        if (t.charAt(0) == ']') return this;
        t = findToken(m, DECL_END, 0);
      } while (t != null);
    }
  }

  public synchronized Ad parseInto(CharSequence cs) {
    return parseInto(AD_START.matcher(cs));
  }

  public static Ad parse(CharSequence cs) {
    return new Ad().parseInto(cs);
  }

  // Access methods
  // --------------
  // Each accessor can optionally have a default value passed as a second
  // argument to be returned if no entry with the given name exists.
  // All of these eventually synchronize on getObject.

  // Get an entry from the ad as a string. Default: null
  public String get(String s) {
    return get(s, null);
  } public String get(String s, String def) {
    Object o = getObject(s);
    return (o != null) ? o.toString() : def;
  }

  // Get an entry from the ad as an integer. Defaults to -1.
  public int getInt(String s) {
    return getInt(s, -1);
  } public int getInt(String s, int def) {
    Number n = getNumber(s);
    return (n != null) ? n.intValue() : def;
  }

  // Get an entry from the ad as a long. Defaults to -1.
  public long getLong(String s) {
    return getLong(s, -1);
  } public long getLong(String s, long def) {
    Number n = getNumber(s);
    return (n != null) ? n.longValue() : def;
  }

  // Get an entry from the ad as a double. Defaults to -1.
  public double getDouble(String s) {
    return getDouble(s, -1);
  } public double getDouble(String s, double def) {
    Number n = getNumber(s);
    return (n != null) ? n.doubleValue() : def;
  }

  // Get an entry from the ad as a Number object. Attempts to cast to a
  // number object if it's a string. Defaults to null.
  public Number getNumber(String s) {
    return getNumber(s, null);
  } public Number getNumber(String s, Number def) {
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
  public boolean getBoolean(String s) {
    return getBoolean(s, false);
  } public boolean getBoolean(String s, boolean def) {
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
  public Ad getAd(String s) {
    return getAd(s, null);
  } public Ad getAd(String s, Ad def) {
    Object o = getObject(s);
    return (s != null && o instanceof Ad) ? (Ad)o : def;
  }
    
  // Look up an object by its key. Handles recursive ad lookups.
  private synchronized Object getObject(String key) {
    return getObject2(key.toLowerCase());
  } private synchronized Object getObject2(String key) {
    if (key == null)
      throw PE("null key given");
    if (key.isEmpty()) return null;
    int i = key.indexOf('.');

    if (i < 0)
      return map.get(key);
    Object o = map.get(key.substring(0,i));
    if (o instanceof Ad)
      return ((Ad)o).getObject(key.substring(i+1));
    return null;
  }

  // Insertion methods
  // -----------------
  // Methods for putting values into an ad. Certain primitive types can
  // be stored as their wrapped equivalents to save space, since they are
  // still printed in a way that is compatible with the language.
  // All of these eventually synchronize on putObject.
  public Ad put(String key, int value) {
    return putObject(key, Integer.valueOf(value));
  }

  public Ad put(String key, long value) {
    return putObject(key, Long.valueOf(value));
  }

  public Ad put(String key, double value) {
    return putObject(key, Double.valueOf(value));
  }

  public Ad put(String key, boolean value) {
    return putObject(key, Boolean.valueOf(value));
  }

  public Ad put(String key, Number value) {
    return putObject(key, value);
  }

  public Ad put(String key, String value) {
    if (!PRINTABLE.matcher(value).matches())
      throw PE("non-printable characters in string");
    return putObject(key, value);
  }

  // If you create a loop with this and print it, I feel bad for you. :)
  public Ad put(String key, Ad value) {
    return putObject(key, value);
  }

  // Use this to insert objects in the above methods. This takes care
  // of validating the key so accidental badness doesn't occur.
  private synchronized Ad putObject(String key, Object value) {
    return putObject2(key.toLowerCase(), value);
  } private synchronized Ad putObject2(String key, Object value) {
    if (key == null)
      throw PE("null key given");
    if (key.isEmpty())
      throw PE("empty key given");
    int i = key.indexOf('.');

    // If we're not doing a sub-ad insertion, just try inserting value.
    if (i < 0)
      return putObject3(key, value);

    // Otherwise try to find ad to insert into, creating if necessary.
    String pk = key.substring(0,i), sk = key.substring(i+1);
    Object o = map.get(pk);

    if (o == null)
      putObject3(pk, new Ad().putObject(sk, value));
    else if (o instanceof Ad)
      ((Ad)o).putObject2(sk, value);
    else
      throw PE("key path contains non-ad");
    return this;
  }

  // Actually puts the object, checking if the key is valid. If value is
  // null, removes the key.
  private synchronized Ad putObject3(String key, Object value) {
    if (!DECL_NAME.matcher(key).matches())
      throw PE("bad key syntax");
    if (value != null)
      map.put(key, value);
    else
      map.remove(key);
    return this;
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
  public synchronized Ad mergeInto(Ad... ads) {
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

  private synchronized void removeKey(String k) {
    int i = k.lastIndexOf('.');
    Ad ad;

    if (i < 0)
      map.remove(k);
    else if ((ad = getAd(k.substring(0,i))) != null)
      ad.removeKey(k.substring(i+1));
  }

  // Composition methods
  // ------------------
  // Methods for presenting and serializing ads.

  // Represent this ad as a nicely-formatted string.
  public synchronized String toString() {
    return toString(true);
  }

  // Represent this ad in a compact way for serialization.
  public synchronized String serialize() {
    return toString(false);
  }

  public synchronized String toString(boolean pretty) {
    StringBuffer sb = new StringBuffer();
    String s = !pretty ? "" : (map.size() > 1) ? "\n" : " ";  // separator
    String i = (pretty && map.size() > 1) ? "  " : "";  // indentation
    String q = !pretty ? "=" : " = ";  // assignment sign
    Iterator<Map.Entry<String,Object>> it = map.entrySet().iterator();
    sb.append("["+s);
    while (it.hasNext()) {
      Map.Entry<String,Object> e = it.next();
      sb.append(i+e.getKey()+q+stringify(e.getValue(), pretty));
      if (it.hasNext()) sb.append(';');
      if (pretty) sb.append(s);
    }
    sb.append("]");
    return sb.toString();
  }

  // Turns entries into strings suitable for printing/serializing.
  private static String stringify(Object v, boolean p) {
    if (v instanceof String) {  // We need to escape the string.
      String s = v.toString().replace("\\", "\\\\").replace("\"", "\\\"");
      return '"'+s+'"';
    } if (v instanceof Ad) {  // We need to indent (if pretty printing).
      String s = ((Ad)v).toString(p);
      return (p ? s.replace("\n", "\n  ") : s);
    } return v.toString();
  }

  public static void main(String[] args) {
    Ad a = new Ad();
    Ad b = new Ad();
    a.put("name", "bob").put("aGe", -30.2E2).put("CASH", "140.53"); a.put("INVENTORY.thING.COunt", 5).put("Inventory.thing2", 2.20);
    a.put("InVeNTory.thINg3", "bobber");
    b.put("name2", "askjdkjasd").put("name", "bob2").put("age2", "4.2E1");
    a.mergeInto(b);
    System.out.println(a);
    System.out.println(a.getInt("age"));
  }
}
