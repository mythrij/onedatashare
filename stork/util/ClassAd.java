package stork.util;

import stork.*;
import stork.util.*;

import java.util.*;
import java.io.*;

import condor.classad.AttrName;
import condor.classad.ClassAdParser;
import condor.classad.ClassAdWriter;
import condor.classad.Constant;
import condor.classad.Expr;
import condor.classad.RecordExpr;

import condor.classad.*;

// This class is a simple wrapper around the more low-level Condor
// ClassAd package. The intent is to make ClassAds less cumbersome to use.

public class ClassAd implements Iterable<String> {
  private RecordExpr record;
  private boolean error = false;

  // This is kind of a hacky thing. These ads are returned by parse when
  // the end of the stream has been reached or an error has occurred.
  public static final ClassAd
    EOF = new ResponseAd("error", "end of stream has been reached"),
    ERROR = new ResponseAd("error", "error parsing ClassAd");

  static { EOF.error = true; ERROR.error = true; }

  // Static methods
  // --------------
  // Parse a ClassAd from a string. Returns null if can't parse.
  public static ClassAd parse(String s) {
    try {
      ClassAdParser parser = new ClassAdParser(s);
      Expr expr = parser.parse();

      if (expr instanceof RecordExpr)
        return new ClassAd((RecordExpr) expr);
      return ERROR;
    } catch (Exception e) {
      return ERROR;
    }
  }

  // Parse a ClassAd from an InputStream. Consumes whitespace until a
  // non-whitespace character is found. Returns null if it can't read or
  // the ad can't be parsed.
  public static ClassAd parse(InputStream is) {
    try {
      boolean escape = false;
      boolean in_str = false;
      int c;
      StringBuffer sb = new StringBuffer(200);

      // Consume whitespaces
      do {
        c = is.read();
      } while (Character.isWhitespace(c));

      // Check if we've reached the end of the stream.
      if (c == -1) return EOF;

      // Check that we're at the beginning of a ClassAd.
      if (c != '[') return ERROR;
      sb.append('[');

      // Read until we get to the end of a ClassAd. Ignore ]'s in strings.
      while (true) {
        c = is.read();
        sb.append((char) c);

        if (c == -1) return EOF;

        if (in_str) {  // Inside of string, ignore ]'s and escaped quotes.
          if (escape) escape = false;  // Still in string if escaped.
          else if (c == '\\') escape = true;
          else if (c == '"') in_str = false;
        } else {  // Not in string, look for string or end ].
          if (c == '"') in_str = true;
          else if (c == ']') break;
        }
      }

      // Try to parse the ad.
      return parse(sb.toString());
    } catch (IOException ioe) {
      return EOF;
    } catch (Exception e) {
      return ERROR;
    }
  }

  // Access methods
  // --------------
  // Each accessor can optionally have a default value passed as a second
  // argument to be returned if no entry with the given name exists.

  // Check if the ClassAd has an entry.
  public boolean has(String s) {
    return record.lookup(s) != null;
  }

  // Get an entry from the ad as a string. Defaults to null.
  public String get(String s) {
    return get(s, null);
  } public String get(String s, String def) {
    Expr e = record.lookup(s);

    if (e == null)
      return def;
    if (e.type == Expr.STRING)
      return e.stringValue();
    else
      return e.toString();
  }

  // Get an entry from the ad as an integer. Defaults to -1.
  public int getInt(String s) {
    return getInt(s, -1);
  } public int getInt(String s, int def) {
    Expr e = record.lookup(s);

    if (e != null) switch (e.type) {
      case Expr.INTEGER:
        return e.intValue();
      case Expr.REAL:
        return (int) e.realValue();
      case Expr.STRING:
        return Integer.parseInt(e.stringValue());
      default:
        return Integer.parseInt(e.toString());
    } return def;
  }

  // Get an entry from the ad as a double. Defaults to NaN on error.
  public double getDouble(String s) {
    return getDouble(s, Double.NaN);
  } public double getDouble(String s, double def) {
    Expr e = record.lookup(s);

    if (e != null) switch (e.type) {
      case Expr.REAL:
      case Expr.INTEGER:
        return e.realValue();
      case Expr.STRING:
        return Double.parseDouble(e.stringValue());
      default:
        return Double.parseDouble(e.toString());
    } return def;
  }

  // Get an entry from the ad as a boolean value. Returns true if the value
  // of the entry is "true" (case insensitive), false otherwise.
  public boolean getBoolean(String s) {
    return getBoolean(s, false);
  } public boolean getBoolean(String s, boolean def) {
    Expr e = record.lookup(s);
    return (e != null) ? e.isTrue() : def;
  }

  // Get an entry as a Condor ClassAd Expr in case someone wants that.
  // Returns null if nothing is found.
  public Expr getExpr(String s) {
    return record.lookup(s);
  }

  // Methods for adding/removing entries
  // -----------------------------------
  // Add new entries to the ClassAd. Return this ClassAd.
  public ClassAd insert(String k, String v) {
    insert(k, Constant.getInstance(v));
    return this;
  }
    
  public ClassAd insert(String k, int v) {
    insert(k, Constant.getInstance(v));
    return this;
  }
    
  public ClassAd insert(String k, double v) {
    insert(k, Constant.getInstance(v));
    return this;
  }
    
  public ClassAd insert(String k, Expr v) {
    if (v != null)
      record.insertAttribute(k, v);
    else
      record.removeAttribute(AttrName.fromString(k));
    return this;
  }

  // Delete an entry from this ClassAd. Return this ClassAd.
  public ClassAd remove(String k) {
    record.removeAttribute(AttrName.fromString(k));
    return this;
  }

  // Other methods
  // -------------
  // Get the number of attributes in this ClassAd.
  public int size() {
    return record.size();
  }

  // Return a new ClassAd that is this ad and another ad merged together.
  // Attributes in both ads will contain values from the second ad.
  public ClassAd merge(ClassAd ad) {
    return new ClassAd(this).importAd(ad);
  }

  public ClassAd importAd(ClassAd ad) {
    // Insert attributes from second ad.
    if (ad != null) for (String s : ad)
      insert(s, ad.getExpr(s));
    return this;
  }

  // Return a new ClassAd based on this one with only fields contained
  // in the filter array.
  public ClassAd filter(String[] filter) {
    ClassAd ad = new ClassAd();

    for (String s : filter)
      if (has(s)) ad.insert(s, getExpr(s));

    return ad;
  }

  // Return true only if an ad contains all the fields in the filter.
  public boolean require(String[] filter) {
    for (String s : filter)
      if (!has(s)) return false;
    return true;
  }

  // Iterator for the internal record.
  public Iterator<String> iterator() {
    return new Iterator<String>() {
      Iterator i = record.attributes();
      public boolean hasNext() { return i.hasNext(); }
      public String next() { return ((AttrName)i.next()).toString(); }
      public void remove() { i.remove(); }
    };
  }

  // True if there was an error parsing this ad.
  public boolean error() { return error; }

  // Convert to a pretty string for printing.
  public String toString(boolean compact) {
    StringWriter sw = new StringWriter();
    int flags = ClassAdWriter.READABLE & ~ClassAdWriter.NO_ESCAPE_STRINGS;

    // Set up ad writer
    ClassAdWriter caw = new ClassAdWriter(sw);

    if (!compact)
      caw.setFormatFlags(flags);

    caw.print(record);
    caw.flush();

    return sw.toString();
  }

  // Default to non-compact representation.
  public String toString() {
    return toString(false);
  }

  // Convert to a compact byte string to send over a socket.
  public byte[] getBytes() {
    return record.toString().getBytes();
  }

  // Constructors
  // ------------
  // Create a new ClassAd that is a clone of the passed ClassAd.
  public ClassAd(ClassAd ad) {
    this();

    for (String s : ad)
      insert(s, ad.getExpr(s));
  }

  // Create a ClassAd object wrapping a RecordExpr.
  public ClassAd(RecordExpr re) {
    record = re;
  }

  // Create an empty ClassAd.
  public ClassAd() {
    record = new RecordExpr();
  }
}
