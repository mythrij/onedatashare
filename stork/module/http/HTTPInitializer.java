package stork.module.http;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.ssl.SslHandler;

/**
 * Convenient HTTP initializer for startup
 */
class HTTPInitializer extends ChannelInitializer<SocketChannel> {

	private boolean ssl;
	private HTTPUtility utility;
	
	public HTTPInitializer (String scheme, HTTPUtility utility) throws HTTPException {
		ssl = false;
		if (scheme == null) {
			throw new HTTPException("Error: null http scheme");
		} else if (scheme.equalsIgnoreCase("https")) {
			ssl = true;
		}
		
		this.utility = utility;
	}
	
	@Override
	protected void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipe = ch.pipeline();
		
		if (ssl) {
			// HTTPs connection
			//SSLEngine sslEng = getSsl(null);
			//pipe.addLast("SSL", new SslHandler(sslEng));
		}
		
		pipe.addLast("Codec", new HttpClientCodec());
		pipe.addLast("Inflater", new HttpContentDecompressor());
		pipe.addLast("Tester", new HTTPTestHandler(utility));
	}

	/* HTTPS transmission
	private SSLEngine getSsl(String proto) throws NoSuchAlgorithmException {
		String protocol = (proto == null) ? "TLS" : proto;
		SSLContext context = SSLContext.getInstance(protocol);
		//TODO https layer
		return null;
	}*/
}
