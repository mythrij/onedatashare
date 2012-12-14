package stork.util;

import java.util.*;

// This class is both an argument parser and a usage information generator.
// Options are individually added with optional argument handlers (see the
// Parser class) and usage descriptions, and will be called upon

public class GetOpts {
  private GetOpts next = null;
  public String   prog = null;
  public String[] args = null;
  public String[] desc = null;
  public String[] foot = null;
  public Parser parser = null;  // Parser for positional parameters.

  private Map<String, Option> by_name;
  private Map<Character, Option> by_char;

  public GetOpts() {
    this(null);
  } public GetOpts(GetOpts next) {
    by_name = new HashMap<String, Option>();
    by_char = new HashMap<Character, Option>();
    if (next != null) {  // Inherit features.
      setNext(next);
      prog = next.prog;
      args = next.args;
      desc = next.desc;
      foot = next.foot;
    }
  }

  // A single option
  public class Option implements Comparable<Option> {
    char c = 0;
    String name;
    String desc = null;
    public Parser parser = null;

    private Option(String name) {
      this.name = name;
    }

    // Get the usage parameters string sans description for this option.
    public String params() {
      String s = (c != 0) ? "  -"+c+", " : "      ";
      String a = parser == null    ? "" :
                 parser.optional() ? "[="+parser.arg()+"]" :
                                     "="+parser.arg();
      return s+"--"+name+a;
    }

    // Convert to a string with usage and description, with appropriate
    // wrapping according to w (first column width) and mw (total width).
    public String toString(int w, int mw) {
      StringBuffer body = new StringBuffer();
      String fmt = "%-"+w+"s%s";
      String col2 = "";
      String head = params();

      if (desc == null) {
        return head;
      } if (head.length()+2 > w) {
        body.append(head).append('\n');
        head = "";
      }

      // Maybe replace this with a matcher? Eh, who cares.
      // Also, allow that 78 (usual_term_width-2) to be configured!
      for (String s : desc.split("\\s+")) {
        if (!col2.isEmpty() && col2.length()+s.length() >= mw-w) {
          body.append(String.format(fmt, head, col2)).append('\n');
          head = "";
          col2 = s;
        } else {
          col2 = (col2.isEmpty()) ? s : col2+' '+s;
        }
      }
      if (!col2.isEmpty())
        body.append(String.format(fmt, head, col2));

      return body.toString();
    }

    public boolean equals(Object o) {
      if (this == o) return true;
      if (this instanceof Option)
        return hashCode() == ((Option)o).hashCode();
      return false;
    }

    public int compareTo(Option o) {
      return name.compareTo(o.name);
    }

    public int hashCode() {
      return (c+name).hashCode();
    }
  }

  // Interface for something that will parse the argument to an option.
  // Attach one of these to an Option to allow it to take an argument.
  // One of these can also be attached to a GetOpts to perform parsing
  // of positional parameters after the end of options.
  public abstract class Parser {
    public String arg() { return ""; }  // Optional usage info.
    public boolean optional() { return false; }
    public abstract ClassAd parse(String s) throws Exception;
  }

  // Just return a ClassAd with the passed argument in it.
  public class SimpleParser extends Parser {
    String name, arg;
    boolean optional;
    public SimpleParser(String n, String a, boolean opt) {
      name = n; arg = a; optional = opt;
    }
    public String arg() { return arg; }
    public boolean optional() { return optional; }
    public ClassAd parse(String s) throws Exception {
      if (s != null)
        return new ClassAd().insert(name, s);
      if (optional)
        return new ClassAd().insert(name, true);
      throw new Error(name+" requires an argument!!");
    }
  }

  // Add an option. To add a parser to the option, immediately after the
  // call to add, reference the parser member of the returned option and
  // create an anonymous opts parser class right there.
  public Option add(String n, String desc) {
    return add('\000', n, desc);
  } public Option add(char c, String n, String desc) {
    Option o = new Option(n);
    o.c = c; o.desc = desc;

    if (n == null || n.isEmpty())
      throw new Error("Option name cannot be null or empty!");
    if (c != 0 && by_char.containsKey(c))
      throw new Error("Option already exists with short name: "+c);
    if (n != null && by_name.containsKey(n))
      throw new Error("Option already exists with name: "+n);
    if (c != 0)
      by_char.put(c, o);
    by_name.put(n, o);
    return o;
  }

  // Check we're not forming a loop with the chain.
  private void checkChain(GetOpts n) {
    if (n == this)  // no no NO NO
      throw new Error("Trying to create a loop in the GetOpt chain!");
    if (next != null)
      next.checkChain(n);
  }

  public void setNext(GetOpts n) {
    if (n != next) checkChain(n);
    next = n;
  }

  // Parse command line arguments, returning a ClassAd with the information
  // parsed from the argument array, throwing an error if there's a problem.
  public ClassAd parse(String[] args) throws Exception {
    int i, j, n = args.length;
    String s, a = null;
    String[] sa;
    char[] c;
    ClassAd ad = new ClassAd();
    Option o = null;
    Parser p = null;

    // Check all of the options, stopping when -- is found or end.
    for (i = 0; i < n; i++) {
      // See if it's -- (end of args).
      if (args[i].equals("--")) break;

      // See if it's a long option or short option.
      if (args[i].startsWith("--")) {  // Long.
        sa = args[i].substring(2).split("=", 2);
        s = sa[0];
        a = (sa.length > 1) ? sa[1] : (i+1 < n) ? args[i+1] : null;
        o = get(s);
        if (o == null)
          throw new Exception("unrecognized option: '"+s+"'");
        if (ad.has(s))
          throw new Exception("duplicate option: '"+s+"'");
        p = o.parser;
        if (p == null)
          ad.insert(s, true);
        else if (a == null && !p.optional())
          throw new Exception("argument required for '"+s+"'");
        else
          ad.importAd(p.parse(a));
        if (sa.length <= 1 && i+1 < n)  // We consumed an arg.
          i++;
      } else if (args[i].startsWith("-")) {  // Short.
        c = args[i].toCharArray();
        for (j = 1; j < c.length; j++) {
          o = get(c[j]);
          if (o == null)
            throw new Exception("unrecognized option: '"+c[j]+"'");
          s = o.name;
          if (ad.has(s))
            throw new Exception("duplicate option: '"+c[j]+"' ("+s+")");
          p = o.parser;
          a = (j+1 < c.length) ? args[i].substring(j+1) :
              (i+1 < n) ? args[i+1] : null;
          if (p == null)
            ad.insert(s, true);
          else if (a == null && !p.optional())
            throw new Exception("argument required for '"+c[j]+"'");
          else
            ad.importAd(p.parse(a));
          if (j+1 >= c.length && i+1 < n)  // We consumed an arg.
            i++;
        }
      } else break;  // End of options.
    }

    // Now pass rest of options to positional parameter parser. XXX
    //if (parser != null)
      //parser.parse(args);

    return ad;
  }

  // Get a handler by name, checking down the chain. Returns null if
  // none found.
  private Option get(String name) {
    Option o = by_name.get(name);
    if (o == null && next != null)
      return next.get(name);
    return o;
  }

  // Likewise, get by short name.
  private Option get(char c) {
    Option o = by_char.get(c);
    if (o == null && next != null)
      return next.get(c);
    return o;
  }

  // Get a set of all options in the chain.
  private Set<Option> getOptions() {
    Set<Option> os = new TreeSet<Option>(by_name.values());
    if (next != null)
      os.addAll(next.getOptions());
    return os;
  }

  // Print usage information then exit.
  public void usageAndExit(int rc, String msg) {
    usage(msg);
    System.exit(rc);
  }

  // Pretty print the usage information and optional message.
  public void usage(String msg) {
    int wrap = 78;  // TODO: Detect screen width/allow configuration.
    StringBuffer body = new StringBuffer();
    Set<Option> op_set = getOptions();

    if (msg != null) {
      body.append(StorkUtil.wrap(msg, wrap));
    }

    if (prog != null) {
      if (body.length() != 0) body.append("\n\n");
      String first =  "Usage: "+prog+' ';
      String rest = "\n    or "+prog+' ';
      body.append(first);
      if (args != null)
        body.append(StorkUtil.joinWith(rest, (Object[]) args));
    }

    if (desc != null) {
      for (String s : desc)
        body.append("\n\n"+StorkUtil.wrap(s, wrap));
    }

    if (!op_set.isEmpty()) {
      if (body.length() != 0) body.append("\n\n");
      body.append("The following options are available:");
      for (Option o : op_set)
        body.append('\n'+o.toString(wrap/3, wrap));
    }

    if (foot != null) {
      for (String s : foot)
        body.append("\n\n"+StorkUtil.wrap(s, wrap));
    }

    System.out.println(body);
  }
}
