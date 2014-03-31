package stork.net;

import java.net.*;
import java.util.*;
import java.io.*;
import java.nio.file.*;

import io.netty.buffer.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;
import io.netty.handler.codec.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.*;
import io.netty.handler.stream.*;
import io.netty.util.*;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static io.netty.handler.codec.http.HttpMethod.*;

import stork.ad.*;
import stork.feather.URI;
import stork.feather.Path;
import stork.scheduler.*;

/**
 * A representation of an incoming HTTP request. This object provides
 * asynchronous accessors for request metadata.
 */
public class HTTPRequest {
  public final String method;
  public final URI uri;

  public HTTPRequest(String method, URI uri) {
    this.method = method;
    this.uri = uri.makeImmutable();
  }
}
