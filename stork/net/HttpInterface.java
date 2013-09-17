package stork.net;

import stork.ad.*;
import stork.scheduler.*;

import io.netty.buffer.*;
import io.netty.handler.codec.*;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.*;
import io.netty.channel.*;
import io.netty.channel.socket.*;

import static io.netty.handler.codec.http.HttpHeaders.Names.*;
import static io.netty.handler.codec.http.HttpHeaders.*;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.*;
import static io.netty.handler.codec.http.HttpMethod.*;

import java.net.*;
import java.util.*;

// Basic REST/HTTP interface.

public class HttpInterface extends StorkInterface {
  private final boolean https;

  public HttpInterface(StorkScheduler s, URI uri) {
    super(s, uri);
    https = "https".equalsIgnoreCase(uri.getScheme());
    name = https ? "HTTPS" : "HTTP";
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

  // Get the command from a path. Throws if the path is invalid or remote.
  private URI getRequestUri(String path) {
    try {
      URI bu = new URI(uri.getPath()+"/").normalize();
      URI pu = bu.relativize(new URI(path));

      if (pu.getScheme() != null || pu.getHost() != null)
        throw new HttpException(NOT_FOUND);
      return pu;
    } catch (URISyntaxException e) {
      throw new HttpException(BAD_REQUEST);
    }
  }

  // Check that the handler allows the method.
  private boolean checkMethod(HttpMethod m, StorkScheduler.CommandHandler h) {
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
  private class HttpAdDecoder extends MessageToMessageDecoder<HttpRequest> {
    public void decode(ChannelHandlerContext ctx, HttpRequest req, List<Object> out) {
      // Get the command from the request.
      URI cmd = getRequestUri(req.getUri());

      // Make sure the command exists and supports the method.
      StorkScheduler.CommandHandler handler = sched.handler(cmd.getPath());
      if (handler == null)
        throw new HttpException(NOT_FOUND);
      if (!checkMethod(req.getMethod(), handler))
        throw new HttpException(METHOD_NOT_ALLOWED);

      String cookie = req.headers().get("Cookie");
      Ad ad = new Ad();

      // Use cookies as a base.
      if (cookie != null)
        ad.addAll(cookiesToAd(cookie));

      // Merge in query string.
      if (cmd.getQuery() != null)
        ad.addAll(queryToAd(cmd.getQuery()));

      // Merge in request body parameters.
      //if (u.get

      ad.put("command", cmd.getPath());

      System.out.println(ad);

      out.add(ad);
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
  private class HttpAdEncoder extends MessageToMessageEncoder<Ad> {
    public void encode(ChannelHandlerContext ctx, Ad ad, List<Object> out) {
      ByteBuf b = Unpooled.copiedBuffer(ad.toJSON().getBytes());
      FullHttpResponse r = new DefaultFullHttpResponse(HTTP_1_1, OK, b);
      r.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
      out.add(r);
    }
  }

  public void init(Channel ch, ChannelPipeline pl) {
    pl.addLast(new HttpServerCodec());
    pl.addLast(new HttpContentCompressor());
    pl.addLast(new HttpAdDecoder());
    pl.addLast(new HttpAdEncoder());
  }

  public int defaultPort() {
    return https ? 443 : 80;
  }
}
