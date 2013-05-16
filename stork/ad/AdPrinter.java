package stork.ad;

import java.util.*;

// Class for rendering ads in different ways.

public class AdPrinter {
  // Options
  public boolean pretty = true;  // Use pretty spacing.
  public boolean multi  = true;  // Print linked ads.
  public boolean dots   = true;  // Compact printing of sub ad IDs.

  String
    LB  = "[",   // Left bracket.
    RB  = "]",   // Right bracket.
    JP  = "|",   // Ad joiner.
    EQ  = "=",   // Assignment token.
    SC  = ";",   // Declaration separator.
    IDL = "",    // Left ID decorator.
    IDR = "",    // Right ID decorator.
    STL = "\"",  // Left string decorator.
    STR = "\"",  // Right string decorator.
    DOT = ".";   // Dotted ID separator.

  // Some default printers.
  public static AdPrinter
    PRETTY    = new AdPrinter(),
    COMPACT   = new AdPrinter().pretty(false),
    JSON      = new AdPrinter().JSON(),
    BARE      = new AdPrinter().bare(),
    BARE_JSON = new AdPrinter().JSON().bare();

  // Change the settings.
  public AdPrinter pretty(boolean p) {
    pretty = p;
    return this;
  } public AdPrinter multi(boolean m) {
    multi = m;
    return this;
  } public AdPrinter dots(boolean d) {
    dots = d;
    return this;
  } public AdPrinter bare() {
    LB = ""; RB = "";
    pretty = false;
    return this;
  } public AdPrinter JSON() {
    LB  = "[{";  RB  = "}]"; JP  = "},{";
    EQ  = ":";  SC  = ",";
    IDL = "\""; IDR = "\"";
    dots = false;
    pretty = false;
    return this;
  }

  // Do the deed.
  public String toString(Ad ad) {
    return append(new StringBuilder(), ad).toString();
  } public byte[] toBytes(Ad ad) {
    return append(new StringBuilder(), ad).toString().getBytes();
  } public StringBuilder append(StringBuilder sb, Ad ad) {
    sb.append(LB);
    String j = "";
    for (Ad a : ad) {
      appendEntries(sb.append(j), a);
      j = JP;
    } return sb.append(RB);
  }

  // Helpers
  // -------
  // Print the key/value entries into buffer.
  private StringBuilder appendEntries(StringBuilder sb, Ad ad) {
    Map<String,Object> map = ad.map;
    Iterator<Map.Entry<String,Object>> it = map.entrySet().iterator();
    String i = (pretty && map.size() > 1) ? "  " : "";  // indentation
    String s = !pretty ? "" : (map.size() > 1) ? "\n" : " ";  // separator
    sb.append(s);
    while (it.hasNext()) {
      Map.Entry<String,Object> e = it.next();
      sb.append(i+IDL+e.getKey()+IDR+stringify(e.getValue()));
      if (it.hasNext()) sb.append(SC);
      if (pretty) sb.append(s);
    } return sb;
  }

  // Translate a Java string into an escaped string for presentation.
  // TODO: Different presentation formats might have different escaping
  // semantics.
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

  // Turns entries into strings suitable for printing/serializing.
  // TODO: Buggy for JSON.
  private String stringify(Object v) {
    boolean p = pretty;
    String q = !p ? EQ : " "+EQ+" ";  // assignment sign
    if (v instanceof String) {  // We need to escape the string.
      String s = escapeString(v.toString());
      return q+STL+s+STR;
    } if (v instanceof Ad) {  // We need to indent (if pretty printing).
      Ad a = (Ad)v;
      if (a.size() == 1 && !a.hasNext() && dots)
      for (String k : a.map.keySet()) {
        Object o = a.map.get(k);
        if (o instanceof Ad)
          return '.'+k+stringify(o);
        return '.'+k+stringify(a.map.get(k));
      } String s = toString(a);
      return q+(p ? s.replace("\n", "\n  ") : s);
    } return q+v.toString();
  }
}
