package stork.module.gridftp;

import stork.util.*;

import java.net.*;
import java.util.*;
import java.io.*;

import org.globus.ftp.*;

// Our slightly better HostPort, though it only works with IPv4.

public class BetterHostPort extends HostPort {
  public byte[] bytes;  // Only the first four bytes of this are used.
  public int port;
  public BetterHostPort(String csv) {
    try {
      bytes = new byte[6];
      int i = 0;
      for (String s : csv.split(","))
        bytes[i++] = (byte) Short.parseShort(s);
      if (i != 6)
        throw new Exception(""+i);
      port = ((bytes[4]&0xFF)<<8) + (bytes[5]&0xFF);
    } catch (Exception e) {
      throw new RuntimeException("malformed PASV reply", e);
    }
  } public int getPort() {
    return port & 0xFFFF;
  } public String getHost() {
    return (bytes[0]&0xFF)+"."+(bytes[1]&0xFF)+
      "."+(bytes[2]&0xFF)+"."+(bytes[3]&0xFF);
  } public String toFtpCmdArgument() {
    return (bytes[0]&0xFF)+","+(bytes[1]&0xFF)+
      ","+(bytes[2]&0xFF)+","+(bytes[3]&0xFF)+
      ","+((port&0xFF00)>>8)+","+(port&0xFF);
  } public void subnetHack(byte[] b) {
    // Make sure the first three octets are the same as the control
    // channel IP. If they're different, assume the server is a LIAR.
    // We should try connecting to the control channel IP. If only the
    // last octet is different, then don't worry, it probably knows
    // what it's talking about. This is to fix issues with servers
    // telling us their local IPs and then us trying to connect to it
    // and waiting forever. This is just a hack and should be replaced
    // with something more accurate, or, better yet, test if we can act
    // as a passive mode client and have them connect to us, since that
    // would be better and assumably we have control over that.
    if (b[0] == bytes[0])
    if (b[1] == bytes[1])
    if (b[2] == bytes[2])
      return;
    bytes = b;
    Log.fine("Adjusting server IP to: ", getHost());
  }
}
