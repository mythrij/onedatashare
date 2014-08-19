package stork.core.server;

import java.util.*;

import stork.feather.*;

public class SessionCache {
  Map<Session,Session> map = new HashMap<Session, Session>();

  public synchronized Resource take(Resource resource) {
    Session session = take(resource.session);
    if (resource.session == session)
      return resource;
    return resource.reselectOn(session);
  }

  public synchronized Session take(Session session) {
    Session cached = map.get(session);
    if (cached == null)
      return session;
    map.remove(cached);
    return cached;
  }

  public synchronized Session put(final Session session) {
    if (session.isClosed())
      return session;
    Session cached = map.get(session);
    if (cached != null)
      return cached;
    session.onClose(new Bell() {
        public void always() { remove(session); }
        });
    map.put(session, session);
    return session;
  }

  public synchronized Session remove(Session session) {
    map.remove(session);
    return session;
  }
}
