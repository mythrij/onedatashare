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
import stork.feather.*;
import stork.feather.URI;
import stork.feather.Path;
import stork.scheduler.*;

/**
 * A representation of an incoming HTTP request. This object provides accessors
 * for request metadata, as well as a tap/sink-based abstraction layer to the
 * Netty backend.
 */
public abstract class HTTPRequest extends Resource<?,HTTPRequest> {
  private HttpRequest nettyRequest;
  protected URI uri;

  /** Create an HTTPRequest from the given Netty HTTP request. */
  public HTTPRequest(HttpRequest nettyRequest) {
    super(Session.ANONYMOUS);
    this.nettyRequest = nettyRequest;
    this.uri = nettyRequest.getUri();
  }

  /** Create an HTTPRequest representing a body part. */
  private HTTPRequest(String name, HTTPRequest parent) {
    super(name, parent);
  }

  public HTTPRequest select(String name) {
    return new HTTPRequest(name, this);
  }

  /**
   * This will emit the content body as the data for the root resource.
   */
  private class HTTPTap extends Tap<HTTPRequest> {
    HTTPTap() {
      super(HTTPRequest.this, true);
    }

    public void start() {
      initialize(Path.ROOT).new Promise() {
        public void done() {
        } public void fail() {
        }
      };
    }

    public void stop() {
      finalize(Path.ROOT);
    }
  };

  /**
   * Tap for multipart data. This will emit data as it arrives from the client
   * in the request body.  Multipart requests should be treated as a single
   * collection resource.
   */
  private class HTTPMultiTap extends Tap<HTTPRequest> {
    HTTPMultiTap() {
      super(HTTPRequest.this, true);
    }

    public void start() {
      initialize(Path.ROOT);
    }

    public void stop() {
      finalize(Path.ROOT);
    }
  };

  /**
   * This will proxy data to the client. Data resources should be sent directly
   * to the client in the response body. TODO: Collection resources need to be
   * zipped or sent using some similar archival encoding. We can't use
   * multipart responses because not all browsers support it.
   */
  private class HTTPSink extends Sink<HTTPRequest> {
    HTTPSink() { super(HTTPRequest.this); }

    // If this is the root, send a header through Netty.
    public Bell<?> initialize(Relative<Resource> resource) {
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

    public void drain(Relative<Slice> slice) {
      sinkToNetty(new DefaultHttpContent(slice.asByteBuf()));
    }

    public void finalize(Relative<Resource> resource) {
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

  /** Check for a header. */
  public String header(String name) {
    return nettyRequest.headers().get(name);
  }

  public boolean isMultipart() {
    String t = header(CONTENT_TYPE);
    return t != null && t.startsWith("multipart/");
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

  public Bell<Stat> stat() {
    if (isRoot()) {
      Stat s = new Stat();
      s.name = "/";
      s.size = size();
      s.dir = isMultipart();
      s.file = !s.dir;
      return new Bell<Stat>().ring(s);
    } else {
      return null;  // TODO
    }
  }

  public Tap<HTTPRequest> tap() {
    return isMultipart() ? new HTTPMultiTap() : new HTTPTap();
  }

  public Sink<HTTPRequest> sink() {
    return new Bell<Sink>.ring(new HTTPSink());
  }
}
