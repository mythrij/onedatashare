package stork.module.http;

import stork.feather.Bell;
import stork.feather.Path;
import stork.feather.Resource;
import stork.feather.Slice;
import stork.feather.Stat;
import stork.feather.Tap;

/**
 * HTTP resource and tap
 */
public class HTTPResource extends Resource<HTTPSession, HTTPResource> {
	
	// Rung when the first resource response header is received
	private Bell<Stat> statBell = new Bell<Stat> ();
	
	protected HTTPResource(HTTPSession session, Path path) {
		super(session, path);
	}

	@Override
	public HTTPTap tap() {
		return new HTTPTap();
	}
	
	@Override
	public Bell<Stat> stat() {
		return statBell;
	}
	
	public class HTTPTap extends Tap<HTTPResource> {
		
		protected Bell<Void> onStartBell, sinkReadyBell;
		private HTTPBuilder builder;
		private Path resourcePath;

		public HTTPTap() {
			super(HTTPResource.this);
			this.builder = HTTPResource.this.session.builder;
			onStartBell = new Bell<Void> ();
			setPath(path);
		}
		
		@Override 
		public Bell<?> start(Bell bell) {
			if (bell.isFailed())
				return bell;
			sinkReadyBell = bell;
			
			synchronized (builder.getChannel()) {
				if (!builder.isClosed()) {
					HTTPChannel ch = builder.getChannel();
					
					if (builder.isKeepAlive()) {
							ch.addChannelTask(this);
							ch.writeAndFlush(builder.prepareGet(resourcePath));
					} else {
							builder.tryResetConnection(this);
					}
				} else {
					onStartBell.ring(new HTTPException("Http session " +
							builder.getHost() + " has been closed."));
				}
			}
			
			sinkReadyBell.new Promise() {
				public void fail(Throwable t) {
					onStartBell.ring(t);
					finish(t);
				}
			};

			return onStartBell;
		}
		
		public Bell<?> drain(Slice slice) { return super.drain(slice); }
		
		public void finish() { super.finish(); } 
		
		protected boolean hasStat() {
			return statBell.isDone();
		}
		
		protected void setStat(Stat stat) {
			statBell.ring(stat);
		}
		
		protected void setPath(Path path) {
			resourcePath = path;
		}
		
		protected Path getPath() {
			return resourcePath;
		}
	}
}
