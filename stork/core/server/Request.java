package stork.core.server;

import java.util.*;

import stork.ad.*;
import stork.core.handlers.*;
import stork.feather.*;

/**
 * A request made to the scheduler. Handlers should provide an instance of a
 * subclass of {@code Request} via the {@link Handler#requestForm()} method
 * containing additional fields the requestor may supply. When the request has
 * been handled, the handler should call {@code ring()} with an optional value
 * that will be marshalled and sent to the requestor to complete the request.
 */
public abstract class Request extends Bell<Object> {
  /** The command given with the request. */
  public transient String command;

  /** The handler which will handle this request. */
  public transient Handler handler;

  /** The user who made the request. */
  public transient User user;

  /** The server this request was made to. */
  public transient Server server;

  /** Whether or not the handler is allowed to change state. */
  public transient boolean mayChangeState = false;

  /** A resource representing a communication channel with the requestor. */
  public transient Resource resource;

  /** The original unmarshalled request. */
  private transient Ad ad = new Ad();

  /** Marshal this request into another request. */
  public <R extends Request> R marshalInto(R request) {
    Ad ad = Ad.marshal(this);
    ad.merge(this.ad);

    request.command = command;
    request.user = user;
    request.server = server;
    request.mayChangeState = mayChangeState;
    request.resource = resource;

    return (R) request.unmarshalFrom(ad);
  }

  /** Marshal an object into this request. */
  public Request unmarshalFrom(Object object) {
    return ad.merge(Ad.marshal(object)).unmarshal(this);
  }

  /** Handle the request. */
  public void handle() {
    try {
      handler.handle(this);
    } catch (Exception e) {
      ring(e);
    }
  }

  public Ad asAd() {
    return Ad.marshal(this).merge(ad);
  }

  public void assertLoggedIn() {
    //if (user == null || user.isAnonymous())
      //throw new RuntimeException("Permission denied.");
  }

  public void assertMayChangeState() {
    //if (!mayChangeState)
      //throw new RuntimeException("Permission denied.");
  }
}
