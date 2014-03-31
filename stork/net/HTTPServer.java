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
 * A simple prefix routing HTTP server utility integrated with Feather. This
 * was created primarily to simplify developer deployments and allow greater
 * flexibility in the REST interface.
 * <p/>
 * Note that this class is not related to the HTTP transfer module, and should
 * not be used directly by any module code.
 */
public class HTTPServer {
  private static Map<InetSocketAddress, HTTPServer> servers =
    new HashMap<InetSocketAddress, HTTPServer>();
  private Map<String,Map<Path,Route>> routes =
    new HashMap<String,Map<Path,Route>>();

  /**
   * Return an {@code HTTPServer} bound to the given host and port. If an
   * {@code HTTPServer} has already been instantiated bound to an equivalent
   * address, it will be returned.
   */
  public static synchronized HTTPServer create(String host, int port) {
    InetSocketAddress isa = new InetSocketAddress(host, port);
    HTTPServer server = servers.get(isa);
    if (server == null)
      servers.put(isa, server = new HTTPServer(isa));
    return server;
  }

  // Create and bind an HTTP server to the given socket address.
  private HTTPServer(InetSocketAddress isa) {
    ServerBootstrap sb = new ServerBootstrap();
    sb.channel(NioServerSocketChannel.class);
    sb.group(acceptor, new NioEventLoopGroup());
    sb.childHandler(new ChannelInitializer<ServerSocketChannel>() {
      protected void initChannel(ServerSocketChannel ch) {
        pl.addLast(new HttpServerCodec());
        pl.addLast(new HttpContentCompressor());
        pl.addLast(new HTTPRouter());
      }
    });

    sb.option(ChannelOption.TCP_NODELAY, true);
    sb.option(ChannelOption.SO_KEEPALIVE, true);

    sb.bind(isa);
  }

  private synchronized void addRoute(String[] methods, Route route) {
    if (methods == null || methods.length == 0) {
      methods = new String[] { null };
    } for (String m : methods) {
      List<Route> rm = routes.get(m);
      if (rm == null)
        routes.add(method, rm = new HashMap<Path,Route>());
      rm.put(route.prefix, route);
    }
  }

  /**
   * Instantiate a {@code Route} to add a route handler.
   */
  public abstract class Route {
    public final Path prefix;

    /**
     * Create a routing table entry for the given methods and prefix.
     *
     * @param prefix the path to match this route against; if {@code null},
     * this is assumed to be the root path.
     * @param method the methods to match this route with; if empty or {@code
     * null}, this route is matched against any method.
     */
    public Route(Path prefix, String... method) {
      this.prefix = (prefix == null) ? Path.ROOT : prefix;
      addRoute(method, this);
    }

    /**
     * Subclasses should implement this to handle a request.
     */
    public abstract void handle(HTTPRequest request);
  }

  /**
   * Returns the first matched route. This first checks for a route for the
   * given method and path. If a route cannot be found, it checks for a route
   * for the wildcard (null) method and the path. If a route still cannot be
   * found, it repeats the procedure for the parent of the path. This is done
   * until a route is found or the path has no parent.
   */
  private Route route(String method, Path path) {
    Map<Path,Route> mm = routes.get(method);
    Map<Path,Route> nm = routes.get(null);

    if (mm == null && nm == null) {
      return null;
    } while (true) {
      if (mm != null && mm.hasKey(path))
        return mm.get(path);
      if (nm != null && nm.hasKey(path))
        return mm.get(path);
      if (path.isRoot())
        return null;
      path = path.up();
    }
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
  private class HTTPException extends RuntimeException {
    HttpResponseStatus status;
    public HTTPException(HttpResponseStatus s) {
      this(s, s.toString());
    } public HTTPException(HttpResponseStatus s, String m) {
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
  private class HTTPDecoder extends MessageToMessageDecoder<HttpObject> {
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
        throw new HTTPException(NOT_FOUND);

      // If the match is for a webdoc endpoint, stop.
      if (route.isWebDoc()) return;

      // Get the body length.
      len = getContentLength(head, 0);

      // Make sure the command exists and supports the method.
      handler = sched.handler(route.path);
      if (handler == null)
        throw new HTTPException(NOT_FOUND);
      if (!checkMethod(head.getMethod(), handler))
        throw new HTTPException(METHOD_NOT_ALLOWED);
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
      System.out.println("DATA: "+data);
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
          writeFile(route.file(), ctx);
        else
          out.add(ad.put("command", command));
        ad = new Ad();
        done = false;
      }
    }

    private void writeFile(File file, ChannelHandlerContext ctx) {
      if (file.isDirectory()) {
        file = new File(file, "index.html");
      } if (!file.exists()) {
        ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND));
      } else try {
        HttpResponse r = new DefaultHttpResponse(HTTP_1_1, OK);
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        setContentLength(r, raf.length());

        String mt = Files.probeContentType(Paths.get(file.getPath()));
        if (mt != null) r.headers().set(CONTENT_TYPE, mt);

        ctx.write(r);
        ctx.write(new DefaultFileRegion(raf.getChannel(), 0, raf.length()));
        ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
      } catch (Exception e) {
        e.printStackTrace();
        ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, INTERNAL_SERVER_ERROR));
      }
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) {
      t = t.getCause();
      t.printStackTrace();
      HTTPException he = (t instanceof HTTPException) ?
        (HTTPException) t : new HTTPException(INTERNAL_SERVER_ERROR);
      ctx.writeAndFlush(he.toHttpMessage());
      ctx.close();
    }
  }

  // A codec for converting ads to HTTP responses.
  private class HTTPAdEncoder extends MessageToMessageEncoder<Ad> {
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
  }

  public int defaultPort() {
    return https ? 443 : 80;
  }
}
