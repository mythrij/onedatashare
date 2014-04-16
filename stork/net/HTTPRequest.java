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
 * A representation of an incoming HTTP request. This object provides accessors
 * for request metadata, as well as a tap/sink-based abstraction layer to the
 * Netty backend.
 */
public abstract class HTTPRequest extends Session {
  private HttpRequest nettyRequest;

  /**
   * Create and HTTPRequest from the given Netty HTTP request.
   */
  public HTTPRequest(HttpRequest nettyRequest) {
    super(URI.create(nettyRequest.getUri()));
    this.nettyRequest = nettyRequest;
  }

  /**
   * This will emit data as it arrives from the client in the request body.
   * Multipart requests should be emitted as a single collection resource.
   */
  private class HTTPTap extends Tap {
    HTTPTap() { super(HTTPRequest.this); }

    protected void start() {
      initialize(Path.ROOT);
    }

    public void publicDrain(Path path, Slice slice) {
      drain(path, slice);
    }

    protected void stop() {
      finalize(Path.ROOT);
    }
  };

  /**
   * This will proxy data to the client. Data resources should be sent directly
   * to the client in the response body. TODO: Collection resources need to be
   * zipped or sent using some similar archival encoding. We can't use
   * multipart responses because not all browsers support it.
   */
  private class HTTPSink extends Sink {
    HTTPSink() { super(HTTPRequest.this); }

    // If this is the root, send a header through Netty.
    protected Bell<?> initialize(RelativeResource resource) {
      Bell bell;
      if (!resource.path().isRoot())  // Sigh browsers...
        throw new RuntimeException("Cannot send multiple files.");
      return generateHeader(resource).new Promise() {
        public void done(HttpResponse o) {
          // Send the headers directly.
          sinkToNetty(o);
        } public void fail(Throwable t) {
          // Send a full error response.
          sinkToNetty(errorToHttpMessage(t));
        }
      };
    }

    protected void drain(RelativeSlice slice) {
      sinkToNetty(new DefaultHttpContent(slice.asByteBuf()));
    }

    protected void finalize(RelativeResource resource) {
      sinkToNetty(new DefaultLastHttpContent());
    }
  };

  private HttpVersion version() {
    return nettyRequest.getProtocolVersion();
  }

  private HttpMethod method() {
    return nettyRequest.getMethod();
  }

  /**
   * Generate an HTTP header for another resource based on its metadata.
   */
  private Bell<HttpResponse> generateHeader(Resource resource) {
    return new resource.stat().new PromiseAs<HttpResponse>() {
      public HttpResponse convert(Stat stat) {
        if (stat.dir)
          throw new RuntimeException("Cannot send directories.");
        if (stat.size == 0)
          return new DefaultFullHttpResponse(version, NO_CONTENT);
        HttpResponse r = new DefaultHttpResponse();
        r.headers().set(CONTENT_LENGTH, stat.size);
        r.headers().set(CONTENT_TYPE, "application/octet-stream");
        r.headers().set(CONTENT_DISPOSITION, stat.size);
      }
    };
  }

  /**
   * Create an HTTP response for a throwable.
   */
  private HttpMessage errorToHttpMessage(Throwable t) {
    String message = errorToAd(t).get("message");
    ByteBuf b = Unpooled.copiedBuffer(message.getBytes());
    FullHttpResponse r = new DefaultFullHttpResponse(
      HTTP_1_1, INTERNAL_SERVER_ERROR, b);
    r.headers().set(CONTENT_TYPE, "text/plain");
    return r;
  }

  /**
   * Implement this to write an object to the Netty pipeline. 
   */
  protected abstract void sinkToNetty(HttpObject object);

  /**
   * Called by a Netty channel handler to write a buffer to the sink.
   */
  public void nettyToTap(ByteBuf buf) {
    tap.publicDrain(new Slice(buf));
  }

  /**
   * Check for a header.
   */
  public String header(String name) {
    return nettyRequest.headers().get(name);
  }

  /**
   * Return true if the tap can emit.
   */
  public boolean ready() {
    return tap.running();
  }
  
  /**
   * Get the length from the Content-Length header.
   */
  public long size() {
    try {
      return Long.parseLong(header("Content-Length"));
    } catch (Exception e) {
      return -1;
    }
  }

  protected Bell<Stat> doStat(Resource resource) {
    if (resource != this)
      throw new RuntimeException();
    Stat s = new Stat();
    s.size = size();
    return new Bell<Stat>().ring(s);
  }

  protected Bell<Tap> doTap(Resource resource) {
    if (resource != this)
      throw new RuntimeException();
    return new Bell<Tap>.ring(new HTTPTap());
  }

  protected Bell<Sink> doSink(Resource resource) {
    if (resource != this)
      throw new RuntimeException();
    return new Bell<Sink>.ring(new HTTPSink());
  }
}
