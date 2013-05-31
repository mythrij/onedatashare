package stork.module.gridftp;

import stork.ad.*;

import java.text.*;
import java.util.*;
import java.util.regex.*;
import java.io.*;

// A class for parsing FTP listings. Based heavily on Mozilla's own FTP
// list parsing code, available in their mozilla-central repository,
// under the path:
//
//   netwerk/streamconv/converters/ParseFTPList.cpp

public class FTPListParser {
  String data = null;
  char list_type;

  // Create a parser with an optional known type suggestion.
  public FTPListParser() {
    this((char)0);
  } public FTPListParser(char type) {
    list_type = type;
  }

  // Check if a file should be ignored.
  public boolean ignoreName(String name) {
    return name == null || name.equals(".") || name.equals("..");
  }

  // Split the data so that each entry can be parsed individually.
  private static Pattern line_pattern =
    Pattern.compile("[\\s\\00]*([^\\n\\r\\00]+)");
  public Ad parse(String data) {
    Matcher m = line_pattern.matcher(data);
    AdSorter sorter = new AdSorter("dir", "name");

    while (m.find()) {
      String line = m.group(1);
      Ad ad = parseEntry(line);
      if (ad != null && !ignoreName(ad.get("name")))
        sorter.add(ad);
    } return sorter.getAd();
  }

  // Parse a line from the listing, return as an ad.
  public Ad parseEntry(String line) {
    String[] tokens;
    Ad ad = new Ad();

    if (line.isEmpty())
      return null;

    // Split the line into tokens delimited by whitespace.
    tokens = line.split("\\s+");

    // If we have a cached listing type, jump to the right parser.
    switch (list_type) {
      case 0:  // Unknown type, just check them all.

      case 'E':  // Check for an EPLF listing.
      if (tokens.length >= 2 && tokens[0].startsWith("+")) try {
        // We should tokenize on tab.
        String[] t = line.substring(1).split("\t+", 2);
        String[] facts = t[0].split(",+");
        String name = t[1];

        // Parse facts according to prefixes.
        for (String f : facts)
        if (!f.isEmpty()) try {
          switch (f.charAt(0)) {
            case 'm':  // Modification time.
              ad.put("time", Long.parseLong(f.substring(1))); break;
            case '/':  // It's a directory.
              ad.put("dir", true); break;
            case 'r':  // It's a file.
              ad.put("file", true); break;
            case 's':  // Permissions.
              ad.put("perm", f.substring(1));
          }
        } catch (Exception e) {
          // There was a parse error, just ignore it.
        }

        // Everything else after the tab is the file name.
        ad.put("name", name);
        list_type = 'E';

        return ad;
      } catch (Exception e) {
        // Bad formatting, skip.
        if (list_type != 0) break;
      }

      case 'M':  // Check for an MLSX listing.
      if (tokens.length >= 2) try {
        // We should tokenize on a single space.
        String[] t = line.split(" ", 2);
        String[] facts = t[0].split(";+");
        String name = t[1];

        if (t.length != 2)
          throw null;

        // Parse each fact, splitting at =.
        for (String f : facts) {
          String s[] = f.split("=", 2);
          s[0] = s[0].toLowerCase();

          if (s.length != 2)
            throw null;

          if (s[0].length() < 4) {
            continue;
          } if (s[0].equals("type")) {
            if (s[1].isEmpty())
              return null;
            else if (s[1].equalsIgnoreCase("file"))
              ad.put("file", true);
            else if (s[1].equalsIgnoreCase("dir"))
              ad.put("dir", true);
            else if (s[1].equalsIgnoreCase("cdir"))
              return null;
            else if (s[1].equalsIgnoreCase("pdir"))
              return null;
            else  // It's just something weird. Let's call it a file.
              ad.put("file", true);
          } else if (s[0].equals("size")) {
            ad.put("size", Long.parseLong(s[1]));
          } else if (s[0].equals("perm")) {
            ad.put("perm", s[1]);  // TODO: Parse this.
          }
        }

        // Everything else after the tab is the file name.
        ad.put("name", name);

        list_type = 'M';
        return ad;
      } catch (Exception e) {
        // Bad formatting, skip.
        if (list_type != 0) break;
      }

      case 'V':  // TODO: Check for a VMS listing.
      case 'C':  // TODO: Check for a CMS listing.
      case 'W':  // TODO: Check for a Windows listing.
      case 'O':  // TODO: Check for an OS2 listing.

      case 'U':  // Check for a Unix listing. TODO: Hellsoft support and symlinks.
      if (tokens.length >= 6) try {
        String perm = tokens[0], name;
        long time, size;
        boolean dir = false;

        // Check for permission flags.
        if (perm.length() != 10 && perm.length() != 11)
          throw null;
        if (!perm.matches("[-bcdlpsw?DFam]([-r][-w].){3}.?"))
          throw null;

        // Scan for size token.
        int i;  // Index of size token.
        for (i = tokens.length-5; i > 1; i--) {
          if (tokens[i].matches("[0-9]+"))
          if (tokens[i+1].matches("[A-Za-z]{3}"))
          if (tokens[i+2].matches("[0-9]{1,2}"))
          if (tokens[i+3].matches("[0-9]{4}|[0-9]{1,2}(:[0-9]{2}){1,2}"))
            break;
        } if (i > 1) {
          // Check if it's a directory.
          dir = perm.charAt(0) == 'd' || perm.charAt(0) == 'D';

          // Parse size.
          try {
            size = Long.parseLong(tokens[i]);
          } catch (Exception e) {
            size = -1;
          }

          // Parse time.
          try {
            String d = tokens[i+1]+" "+tokens[i+2]+" "+tokens[i+3];
            if (tokens[i+3].indexOf(":") > 0) {
              DateFormat df = new SimpleDateFormat("MMM d H:mm");
              Calendar stupid = Calendar.getInstance();
              int year = stupid.get(Calendar.YEAR);
              stupid.setTime(df.parse(d));
              stupid.set(Calendar.YEAR, year);
              Calendar calendar = Calendar.getInstance();
              time = stupid.getTimeInMillis()/1000;
            } else {
              DateFormat df = new SimpleDateFormat("MMM d yyyy");
              time = df.parse(d).getTime()/1000;
            }
          } catch (Exception e) {
            time = -1;
          }

          // Kind of silly thing to get the name.
          try {
            String[] t = line.split("\\s+", i+4);
            t = t[t.length-1].split("\\s", 2);
            name = t[1];

            // Fix symlink names.
            if (perm.charAt(0) == 'l') {
              if (name.endsWith("/"))
                dir = true;
              name = name.replaceAll(" -> .*$", "");
            }
          } catch (Exception e) {
            name = tokens[tokens.length-1];
          }

          // Generate the ad.
          ad.put("name", name);
          if (time > 0)
            ad.put("time", time);
          if (size > 0 && !dir)
            ad.put("size", size);
          ad.put(dir ? "dir" : "file", true);

          list_type = 'U';
          return ad;
        }
      } catch (Exception e) {
        // Bad formatting, skip.
        if (list_type != 0) break;
      }

      case 'w':  // TODO: Check for a Windows 16-bit listing.
      case 'D':  // TODO: Check for a /bin/dls listing.
    } return null;
  }

  // Tester: read from stdin and parse.
  public static void main(String[] args) {
    FTPListParser lp = new FTPListParser();

    Scanner scan = new Scanner(System.in);
    scan.useDelimiter("\\Z");
    String data = scan.next();

    //lp.parse(data);
    System.out.println(lp.parse(data));
  }
}
