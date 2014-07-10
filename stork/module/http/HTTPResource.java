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
	private Bell<Stat> statBell;
	
	protected HTTPResource(HTTPSession session, Path path) {
		super(session, path);
		statBell = new Bell<Stat>();
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
		private HTTPUtility utility;
		private Path resourcePath;

		public HTTPTap() {
			super(HTTPResource.this);
			this.utility = HTTPResource.this.session.utility;
			onStartBell = new Bell<Void> ();
			setPath(path);
		}
		
		@Override 
		public Bell start(Bell bell) {
			if (bell.isFailed())
				return bell;
			sinkReadyBell = bell;
			
			synchronized (utility.getChannel()) {
				HTTPChannel ch = utility.getChannel();
				
				if (utility.isKeepAlive()) {
						ch.addChannelTask(this);
						ch.writeAndFlush(utility.prepareGet(resourcePath));
				} else {
						utility.resetConnection(this);
				}
			}
			
			bell.new Promise() {
				public void fail(Throwable t) {
					onStartBell.ring(t);
					finish(t);
				}
			};

			return onStartBell;
		}
		
		public Bell drain(Slice slice) { return super.drain(slice); }
		
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
