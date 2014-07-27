package stork.module.http;

import java.text.ParseException;
import java.util.Date;

import stork.feather.Bell;
import stork.feather.Slice;
import stork.feather.Stat;
import stork.feather.URI;
import stork.module.http.HTTPResource.HTTPTap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.GenericFutureListener;

/**
 * Message handler receiving status.
 */
enum Status {
	Header,
	Content,
	NotFound
}

/**
 * Handles client-side downstream.
 */
class HTTPMessageHandler extends ChannelHandlerAdapter {
	
	private HTTPBuilder builder;
	private HTTPTap tap;
	private Status status = Status.Header;
	
	public HTTPMessageHandler(HTTPBuilder  util) {
		this.builder = util;
	}
	
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

		final HTTPChannel ch = (HTTPChannel) ctx.channel();

    	if (status == Status.Header) {
    		// This is the first packet of response, needs to know the 
    		// requesting resource of it.
    		status = Status.Content;
    		tap = ch.tapQueue.poll();
    	}

    	if (msg instanceof HttpResponse) {
    		HttpResponse resp = (HttpResponse) msg;
    		String connection = resp.headers().get(HttpHeaders.Names.CONNECTION);

			if (connection != null && connection.equals(HttpHeaders.Values.CLOSE)) {
				if (builder.isKeepAlive()) {
					// Normally, this shouldn't happen. It is assumed that
					// a HTTP server always remains in the same connection state.
					for (HTTPTap tap: ch.tapQueue) {
						builder.tryResetConnection(tap);
					}
					builder.setKeepAlive(false);
				}
			}
			
			caseHandler(resp, ch);
    		
			if (status == Status.Content) {
	    		if (!tap.hasStat()) {
	    			// The resource this tap belongs to has not 
	    			// received meta data yet. Do it now.
	    			Stat stat = new Stat(tap.getPath().toString());
	    			String length = resp.headers().
	    					get(HttpHeaders.Names.CONTENT_LENGTH);
	    			Date time = null;
					try {
						time = HttpHeaders.getDate(resp);
					} catch (ParseException e) {
						// This means date metadata is not available
					}
	    			stat.dir = false;
	    			stat.file = true;
	    			stat.link = tap.getPath();
	    			stat.size = (length == null) ? -1l : Long.valueOf(length);
	    			stat.time = (time == null) ? -1l : time.getTime();
	    			tap.setStat(stat);
	    		}
			}
    		
    		ch.setReadable(false);
    		tap.sinkReadyBell.new Promise() {
    			public void done() { ch.setReadable(true); }
    		};
    	}
		
    	if (msg instanceof HttpContent) {
	    	HttpContent content = (HttpContent) msg;
	    	
    		if (status == Status.Content) {
	    		ByteBuf buf = content.content();
	    		Slice slice = new Slice(buf);
	    		
    			// Ring the bell once the received data is ready
    			tap.onStartBell.ring();
	    		
	    		tap.drain(slice);
    		}

    		if (content instanceof LastHttpContent) {
    			if (status == Status.Content) {
    				tap.finish();
    			}
	    		if (builder.isKeepAlive()) {
	    			status = Status.Header;	// Reset status.
	    		}
    		}
    	}
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
    	final HTTPChannel ch = (HTTPChannel) ctx.channel();
    	
    	if (ch.isOpen()) {
				ch.disconnect();
    	}
    	ch.close().addListener(new GenericFutureListener<ChannelFuture>() {

    		@Override
			public void operationComplete(ChannelFuture arg0) throws Exception {
				if (!builder.isKeepAlive()) {
					synchronized(ch) {
						Bell<Void> bell = builder.tapBellQueue.poll();
						if (bell == null) {
							ch.onInactiveBell.ring();
						} else {
							ch.onInactiveBell.promise(bell);
							ch.onInactiveBell.ring();
						}
					}
				} else {
					// close this channel resource
					ch.onInactiveBell.ring();
					builder.close();
				}
			}
		});
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
    	tap.finish();
    	if (cause instanceof ReadTimeoutException) {
    		ctx.fireChannelInactive();
    	} else {
    		cause.printStackTrace();
    		ctx.close();
    	}
    }
    
    /**
     * Handles various abnormal response codes.
     * 
     * @param response response header
     * @param channel receiver channel 
     */
    private void caseHandler(HttpResponse response, HTTPChannel channel) {
    	HttpResponseStatus status = response.getStatus();
		try {
			if (HTTPResponseCode.isMoved(status)) {
				this.status = Status.NotFound;
				// Redirect to new location
				String newLocation =
						response.headers().get(HttpHeaders.Names.LOCATION);
				newLocation = newLocation.startsWith("/") ?
						newLocation.substring(1) : newLocation;
				URI uri;
				if (!newLocation.contains(":")) {
					uri = URI.create(tap.source().uri() + "/" + newLocation);
				} else {
					uri = URI.create(newLocation);
				}
				tap.setPath(uri.path());
				if (builder.isKeepAlive()) {
					channel.addChannelTask(tap);
					channel.writeAndFlush(builder.prepareGet(tap.getPath()));
				} else {
					builder.tryResetConnection(tap);
				}
				throw new HTTPException(
						tap.getPath() + " " + status.toString());
			} else if (HTTPResponseCode.isNotFound(status)) {
				throw new HTTPException(
						tap.source().uri() + " " + status.toString());
			}
		} catch (HTTPException e) {
			System.err.println(e.getMessage());
		}
    }
}