package stork.util;

import java.util.*;
import stork.ad.*;
import static stork.util.StorkUtil.wrap;
import static stork.util.StorkUtil.joinWith;

// This class is both an argument parser and a usage information generator.
// Options are individually added with optional argument handlers (see the
// Parser class) and usage descriptions, and will be called upon

public class GetOpts {
  private GetOpts parent = null;
  public String   prog = null;
  public String[] args = null;
  public String[] desc = null;
  public String[] foot = null;
  public Parser parser = null;  // Parser for positional parameters.

  private Map<String, Option> by_name = new HashMap<String, Option>();
  private Map<Character, Option> by_char = new HashMap<Character, Option>();
  private Map<String, Command> commands = new HashMap<String, Command>();

  // A utility method for making a nicely wrapped option or command list
  // with descriptions, wrapped according to w (first column width) and
  // mw (total width).
  private static String wrapTable(String name, String desc, int w, int mw) {
    StringBuffer body = new StringBuffer();
    String fmt = "%-"+w+"s%s";
    String col2 = "";

    if (desc == null) {
      return name;
    } if (name.length()+2 > w) {
      body.append(name).append('\n');
      name = "";
    }

    // Maybe replace this with a matcher? Eh, who cares.
    for (String s : desc.split("\\s+")) {
      if (!col2.isEmpty() && col2.length()+s.length() >= mw-w) {
        body.append(String.format(fmt, name, col2)).append('\n');
        name = "";
        col2 = s;
      } else {
        col2 = (col2.isEmpty()) ? s : col2+' '+s;
      }
    }

    if (!col2.isEmpty())
      body.append(String.format(fmt, name, col2));

    return body.toString();
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
      return wrapTable(params(), desc, w, mw);
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

  // A subcommand.
  public class Command implements Comparable<Command> {
    String name, desc;

    public Command(String s, String d) {
      name = s;
      desc = d;
    }

    public String toString(int w, int mw) {
      return wrapTable("  "+name, desc, w+2, mw);
    }

    public int compareTo(Command o) {
      return name.compareTo(o.name);
    }

    public int hashCode() {
      return name.hashCode();
    }
  }

  // Interface for something that will parse the argument to an option.
  // Attach one of these to an Option to allow it to take an argument.
  // One of these can also be attached to a GetOpts to perform parsing
  // of positional parameters after the end of options.
  public abstract class Parser {
    public String arg() { return ""; }  // Optional usage info.
    public boolean optional() { return false; }
    public abstract Ad parse(String s);
  }

  // Just return a Ad with the passed argument in it.
  public class SimpleParser extends Parser {
    String name, arg;
    boolean optional;
    public SimpleParser(String n, String a, boolean opt) {
      name = n; arg = a; optional = opt;
    }
    public String arg() { return arg; }
    public boolean optional() { return optional; }
    public Ad parse(String s) {
      if (s != null)
        return new Ad().put(name, s);
      if (optional)
        return new Ad().put(name, true);
      throw new RuntimeException(name+" requires an argument!");
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
      throw new RuntimeException("Option name cannot be null or empty!");
    if (c != 0 && by_char.containsKey(c))
      throw new RuntimeException("Option already exists with short name: "+c);
    if (n != null && by_name.containsKey(n))
      throw new RuntimeException("Option already exists with name: "+n);
    if (c != 0)
      by_char.put(c, o);
    by_name.put(n, o);
    return o;
  }

  // Add a subcommand. Subcommands must have an associated GetOpts which
  // will extend this GetOpts.
  public void addCommand(String n, String d) {
    commands.put(n, new Command(n, d));
  }

  // Check we're not forming a loop with the chain.
  private void checkChain(GetOpts n) {
    if (n == this)
      throw new Error("Trying to create a loop in the GetOpt chain!");
    if (parent != null)
      parent.checkChain(n);
  }

  public GetOpts parent(GetOpts n) {
    if (n != parent) checkChain(n);
    parent = n;
    return this;
  }

  // Get information about this GetOpts, checking down the chain if null.
  public String prog() {
    if (prog == null && parent != null)
      return parent.prog();
    if (parent != null)
      return joinWith(" ", parent.prog(), prog);
    return prog;
  }

  public String[] args() {
    if (args != null)
      return args;
    if (parent != null)
      return parent.args();
    return new String[0];
  }

  public String[] desc() {
    if (desc != null)
      return desc;
    if (parent != null)
      return parent.desc();
    return new String[0];
  }

  public String[] foot() {
    if (foot != null)
      return foot;
    if (parent != null)
      return parent.foot();
    return new String[0];
  }

  // Parse command line arguments, returning a Ad with the information
  // parsed from the argument array, throwing an error if there's a problem.
  public Ad parse(String[] args) {
    int i, j, n = args.length;
    String s, a = null;
    String[] sa;
    char[] c;
    Ad ad = new Ad();
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
          throw new RuntimeException("unrecognized option: '"+s+"'");
        if (ad.has(s))
          throw new RuntimeException("duplicate option: '"+s+"'");
        p = o.parser;
        if (p == null)
          ad.put(s, true);
        else if (a == null && !p.optional())
          throw new RuntimeException("argument required for '"+s+"'");
        else
          ad.merge(p.parse(a));
        if (sa.length <= 1 && i+1 < n)  // We consumed an arg.
          i++;
      } else if (args[i].startsWith("-")) {  // Short.
        c = args[i].toCharArray();
        for (j = 1; j < c.length; j++) {
          o = get(c[j]);
          if (o == null)
            throw new RuntimeException("unrecognized option: '"+c[j]+"'");
          s = o.name;
          if (ad.has(s))
            throw new RuntimeException("duplicate option: '"+c[j]+"' ("+s+")");
          p = o.parser;
          a = (j+1 < c.length) ? args[i].substring(j+1) :
              (i+1 < n) ? args[i+1] : null;
          if (p == null)
            ad.put(s, true);
          else if (a == null && !p.optional())
            throw new RuntimeException("argument required for '"+c[j]+"'");
          else
            ad.merge(p.parse(a));
          if (j+1 >= c.length && i+1 < n)  // We consumed an arg.
            i++;
        }
      } else break;  // End of options.
    }

    // Now pass rest of options to positional parameter parser.
    //if (parser != null)
      //parser.parse(args);

    return ad;
  }

  // Get a handler by name, checking down the chain. Returns null if
  // none found.
  private Option get(String name) {
    Option o = by_name.get(name);
    if (o == null && parent != null)
      return parent.get(name);
    return o;
  }

  // Likewise, get by short name.
  private Option get(char c) {
    Option o = by_char.get(c);
    if (o == null && parent != null)
      return parent.get(c);
    return o;
  }

  // Get a set of all options in the chain.
  private Set<Option> getOptions() {
    Set<Option> os = new TreeSet<Option>(by_name.values());
    if (parent != null)
      os.addAll(parent.getOptions());
    return os;
  }

  // Get a set of all the subcommands of these parser.
  private Set<Command> getCommands() {
    return new TreeSet<Command>(commands.values());
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
    Set<Command> cmd_set = getCommands();

    // Pretty any message if there is one.
    if (msg != null) {
      body.append(wrap(msg, wrap));
    }

    // Print the program usage header.
    String p = prog();
    if (p != null) {
      if (body.length() != 0) body.append("\n\n");
      String first =  "Usage: "+p+' ';
      String rest = "\n    or "+p+' ';
      body.append(first);
      body.append(joinWith(rest, (Object[]) args()));
    }

    // Print any subcommands.
    if (!cmd_set.isEmpty()) {
      int len = 3;
      // Get max length.
      for (Command c : cmd_set)
        if (c.name.length() > len) len = c.name.length();
      len += 4;

      if (body.length() != 0) body.append("\n\n");
      body.append("The following commands are available:");
      for (Command c : cmd_set)
        body.append('\n'+c.toString(len, wrap));
    }

    // Print the description.
    for (String s : desc())
      body.append("\n\n"+wrap(s, wrap));

    // Print the options.
    if (!op_set.isEmpty()) {
      if (body.length() != 0) body.append("\n\n");
      body.append("The following options are available:");
      for (Option o : op_set)
        body.append('\n'+o.toString(wrap/3, wrap));
    }

    // Finally print the footer.
    for (String s : foot())
      body.append("\n\n"+wrap(s, wrap));

    System.out.println(body);
  }
}
