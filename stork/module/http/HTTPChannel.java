package stork.module.http;

import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import stork.feather.Bell;
import stork.module.http.HTTPResource.HTTPTap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.timeout.ReadTimeoutHandler;

/**
 * HTTP channel class, control data transmissions on the channel
 */
public class HTTPChannel extends NioSocketChannel {

	// Queue of resource taps	
    protected Queue<HTTPTap> tapQueue = new ConcurrentLinkedQueue<HTTPTap>();
    // Bell rung when the channel goes to inactive state
    protected Bell<Void> onInactiveBell = new Bell<Void>();
    protected HttpMethod testMethod = HttpMethod.HEAD;

	private boolean readable = true;
	private HTTPUtility utility;

	/* Constructors */
	public HTTPChannel(Channel parent, EventLoop eventLoop, SocketChannel socket) {
		super(parent, eventLoop, socket);
	}
	public HTTPChannel(EventLoop eventLoop, SocketChannel socket) {
		super(eventLoop, socket);
	}
	public HTTPChannel(EventLoop eventLoop) {
		super(eventLoop);
	}
	
	protected void installUtility(HTTPUtility utility) {
		this.utility = utility;
	}
	
	/* Add a new task to the queue before starting receiving data */
	protected void addChannelTask(HTTPTap tap) {
		tapQueue.offer(tap);
	}
	
	protected synchronized void setReadable(boolean readable) {
		this.readable = readable;
	}
	
	protected void clear() {
		tapQueue.clear();
		for (HTTPTap tap : tapQueue) {
			tap.onStartBell.cancel();
		}
		tapQueue.clear();
	}
	
	@Override
	protected int doReadBytes(ByteBuf buf) throws Exception {
		if (readable) {
			return buf.writeBytes(javaChannel(), buf.writableBytes());
		} else {
			return 0;
		}
	}
    
    /* Remove test handler, and install message receiver handler */
    protected void updatePipeline() {
    	pipeline().remove("Tester");
		pipeline().addFirst("Timer", new ReadTimeoutHandler(30));
		pipeline().addLast(
				"Handler", new HTTPMessageHandler(utility));
    }
}
