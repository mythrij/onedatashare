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
 * A basic HTTP interface to tie the scheduler into the HTTP server.
 */
public class HTTPInterface extends StorkInterface {
  private static Map<URI, HTTPInterface> interfaces =
    new HashMap<URI, HTTPInterface>();

  public HTTPInterface(Scheduler s, URI uri) {
    super(s, uri);

    new HTTPServer.Route(uri, "GET", "POST") {
      public void handle(HTTPRequest request) {
        handleRequest(request);
      }
    };
  }

  // Issue the request and relay the response to the client.
  private void handleRequest(final HTTPRequest request) {
    issueRequest(requestToAd(request)).new Promise() {
      public void done(Request sr) {
        sr.new PromiseAs<Ad>() {
          public Ad convert(Object o) {
            // Convert the request response into an ad.
            return Ad.marshal(o);
          } public Ad convert(Throwable t) {
            // Convert an exception into an ad.
            return new Ad("error", errorToAd(t));
          } public void done(Ad ad) {
            // Write the request back to the requestor.
            request.replyWith(ad);
          }
        };
      }
    };
  }

  // Convert an HTTP request to an ad asynchronously.
  private Bell<Ad> requestToAd(final HTTPRequest req) {
    return new Bell<Ad>() {{
      Ad ad = new Ad("command", req.uri().path().name());

      // Use cookie as a base.
      if (req.cookie() != null)
        ad.addAll(cookiesToAd(req.cookie()));

      // Merge in query string.
      if (req.uri().query() != null)
        ad.addAll(queryToAd(req.uri().query()));

      // If there's no body, we're done. Else, parse asynchronously.
      if (!req.hasBody())
        ring(ad);
      else
        handleRequestBody(ad, req).promise(this);
    }};
  }

  // Asynchronously handle an HTTP request body.
  private Bell<Ad> handleRequestBody(final Ad head, HTTPRequest req) {
    // Make sure it's a type we can handle.
    HTTPContentType type = req.type();

    if (type.startsWith("application/json"))
      
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
      HttpException he = (t instanceof HttpException) ?
        (HttpException) t : new HttpException(INTERNAL_SERVER_ERROR);
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
    pl.addLast(new HttpServerCodec());
    pl.addLast(new HttpContentCompressor());
    pl.addLast(new HTTPDecoder());
    pl.addLast(new HTTPAdEncoder());
  }
}
