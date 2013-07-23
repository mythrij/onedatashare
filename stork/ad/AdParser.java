package stork.ad;

import java.io.*;
import java.util.*;
import java.math.*;

public class AdParser {
  int level = 0;
  char saved = 0;
  private Reader r;

  public AdParser(CharSequence s) {
    this(new StringReader(s.toString()));
  } public AdParser(InputStream is) {
    this(new InputStreamReader(is));
  } public AdParser(File f) throws FileNotFoundException {
    this(new FileReader(f));
  } public AdParser(Reader r) {
    this.r = r;
  }

  // Utility methods
  // ---------------
  // Get the next character, throwing an unchecked exception on error.
  private char next() {
    try {
      int i = (saved != 0) ? saved : r.read();
      saved = 0;
      if (i <= -1)
        throw new RuntimeException("end of stream reached");
      return (char) i;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Peek at the next character, then save it.
  private char peek() {
    char c = next();
    return saved = c;
  }

  // Check if a string or range contains a character.
  private static boolean check(char c, String s) {
    return s.indexOf(c) >= 0;
  } private static boolean check(char c, char from, char to) {
    if (from > to) return check(c, to, from);
    return c >= from && c <= to;
  }

  // Discard characters from the given set.
  private char discard() {
    // Discard whitespace by default.
    return discard("\t\n\r\b ");
  } private char discard(String s) {
    for (char c = peek(); check(c, s); c = peek()) next();
    return peek();
  }

  // Discard all until a character is found.
  private char discardTo(String s) {
    char c;
    for (c = next(); !check(c, s); c = next());
    return c;
  }

  // Discard all whitespace and comments.
  private void discardIgnored() {
    while (true) switch (discard()) {
      default: return;
      case '#': discardTo("\r\n");
    }
  }

  // Ignore all whitespace and find a character in the given set. If
  // something else is found, throws a parse error.
  private char find(String s) {
    discardIgnored();
    char c = next();
    if (!check(c, s))
      throw new RuntimeException("unexpected character: "+c);
    return c;
  }

  // Find a character in an inclusive range of characters.
  private char findRange(char s, char e) {
    discardIgnored();
    char c = next();
    if (!check(c, s, e))
      throw new RuntimeException("unexpected character: "+c);
    return c;
  }

  // Lowercase a character assuming it's an A-Z letter.
  private static char low(char c) {
    return (char)((int)c | (int)' ');
  }

  // Parsing methods
  // ---------------
  public Ad parseAd() {
    return parseInto(new Ad());
  }

  public Ad parseInto(Ad ad) {
    find("{[(<");
    for (int i = 0; ; i++) {
      discardIgnored();

      // Check for end of ad or superfluous separators.
      char c = peek();
      if (check(c, "}])>")) {
        next();
        return ad;
      } while (check(c, ",;")) {
        next();
        discardIgnored();
        c = peek();
      }

      // Check if first token is an id or a string.
      Object o = readValue();

      // Check if it's anonymous or not.
      switch (c = find(":=,;}])>")) {
        case ':': // Check for assignment.
        case '=': ad.putObject(o, findValue()); break;
        case '}': // Check for end and push char back if found.
        case ']':
        case ')':
        case '>': saved = c;
        case ',': // Check for separator.
        case ';':
          if (o instanceof Atom)
            ad.putObject(null, ((Atom)o).eval());
          else
            ad.putObject(null, o);
      }
    }
  }

  // Find and unescape a string.
  private String readString() {
    StringBuilder sb = new StringBuilder();
    char c = next();
    if (c != '"')
      throw new RuntimeException("expecting start of string");
    while (true) switch (c = next()) {
      case '"' : return sb.toString();
      case '\\': sb.append(readEscaped()); continue;
      default:
        if (Character.isISOControl(c))
          throw new RuntimeException("illegal character in string");
        sb.append(c);
    }
  }

  // Find an escaped character, assuming the \ has already been read.
  private char readEscaped() {
    switch (next()) {
      case '"' : return '"';
      case '\\': return '\\';
      case '/' : return '/';
      case 'b' : return '\b';
      case 'f' : return '\f';
      case 'n' : return '\n';
      case 'r' : return '\r';
      case 't' : return '\t';
      case 'u' : {
        int r = 0;
        for (int i = 0; i < 4; i++) {
          char c = next();
          if (check(c, '0', '9'))
            r = (r << 4) | (c-'0');
          else if (check(c = low(c), 'a', 'f'))
            r = (r << 4) | (c-'a'+10);
          else throw new RuntimeException("illegal escape sequence");
        } return (char) r;
      }
    } throw new RuntimeException("illegal escape sequence");
  }

  // An atom represents something that can be either an identifier or a
  // keyword.
  private static class Atom {
    String s;
    Atom(String s) { this.s = s; }
    public String toString() { return s; }
    public Object eval() {
      s = s.toLowerCase();
      if (s.equals("false"))
        return Boolean.FALSE;
      if (s.equals("true"))
        return Boolean.TRUE;
      if (s.equals("null"))
        return null;
      throw new RuntimeException("unknown keyword: "+s);
    }
  }

  // Try to read an atom.
  private Atom readAtom() {
    StringBuilder sb = new StringBuilder();
    for (char c = peek(); validAtomPart(c); c = peek())
      sb.append(next());
    return new Atom(sb.toString());
  }

  // Check if a character can start or be in an atom.
  private static boolean validAtomStart(char c) {
    return check(low(c), 'a', 'z') || c == '_';
  } private static boolean validAtomPart(char c) {
    return check(low(c), 'a', 'z') || c == '_' || check(c, '0', '9');
  }

  // Check if a string is a valid identifier.
  public static boolean checkIdentifier(String s) {
    for (char i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (i == 0 && !validAtomStart(c) || !validAtomPart(c))
        return false;
    } return true;
  }

  // Find a value, and return it as a Java object.
  private Object findValue() {
    discardIgnored();
    return readValue();
  } private Object readValue() {
    char c = peek();
    switch (c) {
      case '"': return readString();
      case '-': return readNumber();
    } if (check(c, '0', '9')) {
      return readNumber();
    } if (check(c, "{[(<")) {
      return parseAd();
    } if (validAtomStart(c)) {
      return readAtom();
    } throw new RuntimeException("cannot parse value starting with: "+c);
  }

  private Number readNumber() {
    char c;
    boolean d = false;
    StringBuilder sb = new StringBuilder();
    while (true) switch (c = peek()) {
      case '.':
      case 'e':
      case 'E':
      case '+':
        d = true;
      case '-':
        sb.append(next());
        continue;
      default:
        if (check(c, '0', '9'))
          sb.append(next());
        else if (check(c, ",:; \n\r\t>}])")) {
          return d ? new BigDecimal(sb.toString()):
                     new BigInteger(sb.toString());
        }
        else
          throw new RuntimeException("unexpected character: "+c);
    }
  }
}
