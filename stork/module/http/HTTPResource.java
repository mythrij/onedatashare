package stork.module.http;

import stork.feather.Bell;
import stork.feather.Path;
import stork.feather.Resource;
import stork.feather.Slice;
import stork.feather.Stat;
import stork.feather.Tap;

public class HTTPResource extends Resource<HTTPSession, HTTPResource> {
	
	// Rung when the first resource response header is received
	private Bell<Stat> onHeadBell;
	
	protected HTTPResource(HTTPSession session, Path path) {
		super(session, path);
		onHeadBell = new Bell<Stat>();
	}

	@Override
	public HTTPTap tap() {
		return new HTTPTap();
	}
	
	@Override
	public Bell<Stat> stat() {
		return onHeadBell;
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
				if (utility.isKeepAlive()) {
						utility.getChannel().addChannelTask(this);
						utility.getChannel().writeAndFlush(
								utility.prepareGet(resourcePath));
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
			return onHeadBell.isDone();
		}
		
		protected void setStat(Stat stat) {
			onHeadBell.ring(stat);
		}
		
		protected void setPath(Path path) {
			resourcePath = path;
		}
		
		protected Path getPath() {
			return resourcePath;
		}
	}
}
