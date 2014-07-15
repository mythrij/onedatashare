package stork.module.http;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.GenericFutureListener;
import stork.feather.Bell;
import stork.feather.Path;
import stork.feather.URI;
import stork.module.http.HTTPResource.HTTPTap;

/**
 * HTTP utility that provides necessary functions to the session
 */
class HTTPBuilder {
	// Bell used to indicate the close state of session
    protected final Bell<Void> onCloseBell = new Bell<Void> ();
    // Bell rung when channel passes the connection test before using
    protected Bell<Void> onTestBell;
    // Bell rung when the channel is ready to use
    protected Bell<Void> onConnectBell;
    // Queue that stores all unhandled requested taps
    protected Queue<Bell<Void>> tapBellQueue;
    // Tells the connection state, set by final connection test result 
    protected boolean isKeepAlive = true;
    
    private HTTPChannel channel;
    private Bootstrap boot;
    private URI uri;
    private int port;
    
    // Constructor for base HTTP channel
    public HTTPBuilder(HTTPSession session) {        
        try {
            boot = new Bootstrap();
            boot.group(session.workGroup)
             .channel(HTTPChannel.class)
             .handler(new HTTPInitializer(session.uri.scheme(), this));
            
            // Channel setup
    		onConnectBell = new Bell<Void>();
            setUri(session.uri);
            setupWithTest();
            
            // Tap bells queue setup
            tapBellQueue = new ConcurrentLinkedQueue<Bell<Void>>();
        } catch (HTTPException e) {
        	System.err.println(e.getMessage());
        }
    }
    
    public Bell<?> onClose() {
    	return onCloseBell;
    }
    
    public boolean isClosed() {
    	return onCloseBell.isDone();
    }
    
    /** 
     * Close and clear up the channel 
     */
    public void close() {
    	if (!isClosed()) {
    		tapBellQueue.clear();
    		synchronized (channel) {
		    	channel.clear();
		    	channel.close();
				onCloseBell.ring();
    		}
    	}
    }

    public boolean isKeepAlive() {
    	return isKeepAlive;
    }
    
    protected void setKeepAlive(boolean v) {
        	isKeepAlive = v;
    }
    
    /**
     * Establish a new socket connection with connection test
     */
    protected void setupWithTest() {
		ChannelFuture future = boot.connect(uri.host(), port);
		future.addListener(new GenericFutureListener<ChannelFuture>() {

			@Override
			public void operationComplete(ChannelFuture f) {
				if (f.isSuccess()) {
					channel = (HTTPChannel) f.channel();
					testConnection();
					onTestBell.promise(onConnectBell);
				} else {
					onConnectBell.ring(f.cause());
				}
			}
		});
    }
    
    /**
     * Create a new socket connection in case that keep-alive option 
     * is not available.
     */
    protected Bell<HTTPChannel> tryResetConnection(HTTPTap tap) {
    	// Bell rung when the channel has been established
    	final HTTPTap localTap = tap;
		final Bell<HTTPChannel> connectBell = new Bell<HTTPChannel>() {
			
			@Override
			protected void done() {
				channel.addChannelTask(localTap);
				channel.writeAndFlush(prepareGet(localTap.getPath()));
			}
		};

    	// Start a new connection immediately if the channel is in idle status
    	synchronized (tapBellQueue) {
    		if (channel.onInactiveBell.isDone() && tapBellQueue.isEmpty()) {
    			ChannelFuture f = boot.connect(uri.host(), port);
    			f.addListener(new GenericFutureListener<ChannelFuture>() {

    				@Override
    				public void operationComplete(ChannelFuture f) {
    					if (f.isSuccess()) {
    						channel = (HTTPChannel) f.channel();
    						connectBell.ring(channel);
    					} else {
    						connectBell.ring(f.cause());
    					}
    				}
    			});
    			
    			return connectBell;
    		}
    	}
    	
    	// Otherwise, add the resource request to the waiting queue
    	// Bell rung when the channel finishes previous task
    	Bell<Void> waitBell	= new Bell<Void>();
    	Bell<Void> createBell = new Bell<Void>() {
    		
    		@Override
    		protected void done() {
    			ChannelFuture f = boot.connect(uri.host(), port);
    			f.addListener(new GenericFutureListener<ChannelFuture>() {

    				@Override
    				public void operationComplete(ChannelFuture f) {
    					if (f.isSuccess()) {
    						channel = (HTTPChannel) f.channel();
    						connectBell.ring(channel);
    					} else {
    						connectBell.ring(f.cause());
    					}
    				}
    			});
    		}
    	};
    	
    	waitBell.promise(createBell);
    	tapBellQueue.offer(waitBell);
    	return connectBell;
    }
    
    protected void setUri(URI uri) {
    	String strUri = uri.toString();
    	if (strUri.endsWith("/")) {
    		strUri = strUri.substring(0, strUri.length() - 1);
    	}
    	this.uri = URI.create(strUri);
    	try {
			port = analURI(uri);
		} catch (HTTPException e) {
			System.err.println(e.getMessage());
		}
    }
    
    protected String getHost() {
    	return uri.host();
    }
    
    protected int getPort() {
    	return port;
    }
    
    protected HTTPChannel getChannel() {
    	return channel;
    }
    
    /* Prepare request message to be sent*/
    protected HttpRequest prepareGet(Path path) {
		HttpRequest request = new DefaultFullHttpRequest( 
	    		HttpVersion.HTTP_1_1, HttpMethod.GET, "" + path);
		request.headers().set(HttpHeaders.Names.HOST, this.uri.host());
		request.headers().set(HttpHeaders.Names.USER_AGENT, "Stork");
		
		return request;
    }
    
    /* Test whether the connection could be keep-alive */
    private void testConnection() {
    	HttpRequest request = prepareGet(uri.path());
    	onTestBell = new Bell<Void>() {
    		@Override
    		protected void done() {
    			channel.config().setKeepAlive(isKeepAlive);
    			if (isKeepAlive) {
    				channel.restorePipeline(HTTPBuilder.this);
    			}
    		}
    	};
    	request.setMethod(channel.testMethod);
    	channel.testerPipeline(this);
    	channel.config().setKeepAlive(true);
    	channel.writeAndFlush(request);
    }
    
    /*
     * Return an appropriate port number from given URL
     */
    private int analURI(URI uri) throws HTTPException {
    	int port = -1;
    	
		if (uri.host() == null) {
			throw new HTTPException("Error: null host name");
		}
		
		if (uri.port() == -1) {
			if (uri.scheme().equalsIgnoreCase("http")) {
        		if (uri.port() == -1) {
        			port = 80;
        		}
			} else if (uri.scheme().equalsIgnoreCase("https")) {
				port = 443;
			}
		} else {
			port = uri.port();
		}
		
		if (port == -1) {
			throw new HTTPException("Error: incorrect port number");
		}
		
		return port;
    }
}
