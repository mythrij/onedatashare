package stork.feather.util;

import stork.feather.*;

/**
 * A base class for anonymous {@code Session}s. {@code AnonymousSession}s and
 * {@code AnonymousResource}s are generally used for creating virtual {@code
 * Sink}s and {@code Tap}s that emit {@code Slice}s that aren't based on real
 * addressable {@code Resource}s, yet must declare an associated {@code
 * Resource} to work within the framework. This class represents a virtual
 * {@code Session} such implementations may use.
 */
public class AnonymousSession
extends Session<AnonymousSession,AnonymousResource> {
  /** Create a new {@code AnonymousSession} with an empty URI. */
  public AnonymousSession() {
    super(URI.EMPTY, null);
  }

  public AnonymousResource select(Path path) {
    return new AnonymousResource(this, path);
  }
}
