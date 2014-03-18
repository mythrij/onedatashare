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

// XXX This whole file is a little kludgy... It has an ad hoc routing
// implementation to handle overlapping HTTP resources (typically an API
// resource and a static web interface resource) served over the same port.
// This should probably be cleaned up a little bit.

// An entry in the routing table.
class Route {
  HttpInterface iface;
  URI uri;
  Object end;

  Route(HttpInterface hi, URI uri, Object end) {
    iface = hi;
    this.uri = uri.makeImmutable();
    this.end = end;
  }

  // FIXME when URIs can officially be relatived. This is hacky, slow, and bad.
  RouteMatch match(URI uri) {
    String ep = uri.endpoint();
    if (ep != null && !ep.equals(this.uri.endpoint()))
      return null;
    String q = uri.query();
    uri.query(null).fragment(null);

    Path ip = uri.path();
    if (ip == null) ip = Path.ROOT;
    Path bp = this.uri.path();
    if (bp == null) bp = Path.ROOT;

    // Prefix match
    if (ip.toString().startsWith(bp.toString())) {
      String rp = ip.toString().substring(bp.toString().length());
      return new RouteMatch(rp, q, this);
    }

    return null;
  }
}

// Result of a routing lookup.
class RouteMatch {
  String path, query;
  Route route;

  RouteMatch(String path, String query, Route route) {
    this.path = path;
    this.query = query;
    this.route = route;
  } boolean isWebDoc() {
    return route.end instanceof File;
  } File file() {
    return new File((File) route.end, path);
  }
}

// Simple mechanism for routing HTTP requests.
class Router {
  static List<Route> routes = new LinkedList<Route>();

  // Returns the first matched route.
  static RouteMatch route(URI uri) {
    for (Route r : routes) {
      RouteMatch m = r.match(uri);
      if (m != null) return m;
    } return null;
  }

  // Add a route.
  static void add(Route r) { routes.add(r); }
}

/** Basic HTTP interface. */
public class HttpInterface extends StorkInterface {
  private final boolean https;

  private static Map<URI, HttpInterface> interfaces =
    new HashMap<URI, HttpInterface>();

  // Used to create scheduler interface.
  private HttpInterface(Scheduler s, URI uri) {
    super(s, uri);
    https = "https".equalsIgnoreCase(uri.scheme());
    name = https ? "HTTPS" : "HTTP";
  }

  // Used to create webdoc interface.
  private HttpInterface(String docroot, URI uri) {
    super(null, uri);
    https = "https".equalsIgnoreCase(uri.scheme());
    name = https ? "HTTPS" : "HTTP";
  }

  /**
   * Listen for scheduler API requests at the given URI.
   */
  public static HttpInterface register(Scheduler scheduler, URI uri) {
    URI ep = uri.endpointURI().makeImmutable();
    HttpInterface hi = interfaces.get(ep);
    if (hi == null)
      interfaces.put(ep, hi = new HttpInterface(scheduler, uri));
    else
      hi.sched = scheduler;
    Router.add(new Route(hi, uri, scheduler));
    return hi;
  }

  /**
   * Listen for static document requests at the given URI.
   */
  public static HttpInterface register(String docroot, URI uri) {
    URI ep = uri.endpointURI().makeImmutable();
    HttpInterface hi = interfaces.get(ep);
    if (hi == null)
      interfaces.put(ep, hi = new HttpInterface(docroot, uri));
    Router.add(new Route(hi, uri, new File(docroot)));
    return hi;
  }

  // Convert a cookie string into an ad.
  private Ad cookiesToAd(String cookie) {
    Ad ad = new Ad();
    Set<Cookie> cookies = CookieDecoder.decode(cookie);
    for (Cookie c : cookies)
      ad.put(c.getName(), c.getValue());
    return ad;
  }

  // Convert a query string into an ad.
  private Ad queryToAd(String query) {
    Ad ad = new Ad();
    QueryStringDecoder qsd = new QueryStringDecoder(query, false);
    Map<String, List<String>> map = qsd.parameters();
    for (String k : map.keySet())
      ad.put(k, map.get(k).get(0));
    return ad;
  }

  // Check that the handler allows the method.
  private boolean checkMethod(HttpMethod m, Scheduler.CommandHandler h) {
    return m.equals(POST) || m.equals(GET) && !h.affectsState();
  }

  // Custom exception which wraps an HTTP error code.
  private class HttpException extends RuntimeException {
    HttpResponseStatus status;
    public HttpException(HttpResponseStatus s) {
      this(s, s.toString());
    } public HttpException(HttpResponseStatus s, String m) {
      super(m);
      status = s;
    } public FullHttpResponse toHttpMessage() {
      ByteBuf b = Unpooled.copiedBuffer(getMessage().getBytes());
      FullHttpResponse r = new DefaultFullHttpResponse(HTTP_1_1, status, b);
      r.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
      return r;
    }
  }

  // A codec for converting HTTP requests into ads.
  private class HttpDecoder extends MessageToMessageDecoder<HttpObject> {
    private HttpRequest head;  // Current request header.
    private HttpContent body;  // Current content body.
    private Scheduler.CommandHandler handler;
    private String command;
    private Ad ad = new Ad();
    private long len = 0;
    private boolean done = false;
    private RouteMatch route;

    // Extract information from the header and put it in the ad.
    public void handleHead(HttpRequest head) {
      // Run the requested path through the router.
      route = Router.route(URI.create(head.getUri()));
      if (route == null)
        throw new HttpException(NOT_FOUND);

      // If the match is for a webdoc endpoint, stop.
      if (route.isWebDoc()) return;

      // Get the body length.
      len = getContentLength(head, 0);

      // Make sure the command exists and supports the method.
      handler = sched.handler(route.path);
      if (handler == null)
        throw new HttpException(NOT_FOUND);
      if (!checkMethod(head.getMethod(), handler))
        throw new HttpException(METHOD_NOT_ALLOWED);
      command = route.path;

      String cookie = head.headers().get("Cookie");

      // Use cookies as a base.
      if (cookie != null)
        ad.addAll(cookiesToAd(cookie));

      // Merge in query string.
      if (route.query != null)
        ad.addAll(queryToAd(route.query));
    }

    // Parse body chunks.
    public void handleBody(HttpContent body) {
      String type = head.headers().get("Content-Type");
      String data = body.content().toString(CharsetUtil.UTF_8);
      if (type == null)
        ad.addAll(Ad.parse(data));
      else if (type.startsWith("application/x-www-form-urlencoded"))
        ad.addAll(queryToAd(data));
      else if (type.startsWith("multipart"))
        throw new RuntimeException("multipart messages are not supported");
      else
        ad.addAll(Ad.parse(data));
      done = true;
    }

    public void decode(ChannelHandlerContext ctx, HttpObject obj, List<Object> out) {
      if (obj instanceof HttpRequest) {
        handleHead(head = (HttpRequest) obj);
      } if (obj instanceof HttpContent) {
        handleBody(body = (HttpContent) obj);
      } if (done) {
        if (route.isWebDoc())
          out.add(route.file());
        else
          out.add(ad.put("command", command));
        ad = new Ad();
        done = false;
      }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
      t = t.getCause();
      t.printStackTrace();
      HttpException he = (t instanceof HttpException) ?
        (HttpException) t : new HttpException(INTERNAL_SERVER_ERROR);
      ctx.writeAndFlush(he.toHttpMessage());
      ctx.close();
    }
  }

  // A codec for reading a static file into an HTTP response.
  private class HttpFileEncoder extends MessageToMessageEncoder<File> {
    public void encode(ChannelHandlerContext ctx, File file, List<Object> out) {
      if (!file.exists()) {
        out.add(new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND));
      } else try {
        FullHttpResponse r = new DefaultFullHttpResponse(HTTP_1_1, OK);
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        setContentLength(r, raf.length());
        String mt = Files.probeContentType(Paths.get(file.getPath()));
        if (mt != null) r.headers().set(CONTENT_TYPE, mt);
        out.add(r);
        out.add(new ChunkedFile(raf));
      } catch (Exception e) {
        out.add(new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR));
      }
    }
  }

  // A codec for converting ads to HTTP responses.
  private class HttpAdEncoder extends MessageToMessageEncoder<Ad> {
    public void encode(ChannelHandlerContext ctx, Ad ad, List<Object> out) {
      String error = ad.get("error");
      FullHttpResponse r;
      if (error == null) {
        ByteBuf b = Unpooled.copiedBuffer(ad.toJSON().getBytes());
        r = new DefaultFullHttpResponse(HTTP_1_1, OK, b);
        r.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
      } else {
        ByteBuf b = Unpooled.copiedBuffer(error.getBytes());
        r = new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR, b);
        r.headers().set(CONTENT_TYPE, "text/plain");
      } out.add(r);
    }
  }

  public void init(Channel ch, ChannelPipeline pl) {
    pl.addLast(new HttpServerCodec());
    pl.addLast(new HttpContentCompressor());
    pl.addLast(new HttpDecoder());
    pl.addLast(new HttpAdEncoder());
  }

  public int defaultPort() {
    return https ? 443 : 80;
  }
}
