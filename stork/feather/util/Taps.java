package stork.feather.util;

import java.nio.*;
import java.nio.charset.*;

import io.netty.buffer.*;
import io.netty.util.*;

import stork.feather.*;

/**
 * A utility class for creating anonymous {@link Tap}s for various purposes.
 */
public final class Taps {
  private Taps() { }

  /**
   * Create an anonymous {@code Tap} which emits a single {@code Slice} to an
   * attached {@code Sink}. If {@code slice} is {@code null}, the returned
   * {@code Tap} will initialize the attached {@code Sink}, but will not emit
   * any {@code Slice}s.
   *
   * @param slice the {@code Slice} emitted by the returned {@code Tap}.
   * @return An anonymous {@code Tap} which will emit {@code slice}.
   */
  public static Tap fromSlice(final Slice slice) {
    return Resources.fromSlice(slice).tap();
  }

  /**
   * Create a {@code Tap} which emits the given {@code Slice} for the given
   * {@code Resource} {@code root}.
   */
  public static <R extends Resource>
  Tap<? super R> fromSlice(R root, Slice slice) {
    final Slice s = slice;
    return new Tap<R>(root) {
      private final Tap<R> thisTap = this;
      public Bell<?> start() {
        initialize(Path.ROOT).new Promise() {
          public void done() {
            if (s != null) drain(s);
            thisTap.finalize(Path.ROOT);
          }
        };
        return null;
      }
    };
  }

  /**
   * Create an anonymous {@code Tap} which emits the {@code String}
   * representation of an object encoded as UTF-8.
   *
   * @param object the object to stringify and emit.
   * @return An anonymous {@code Tap} which will emit {@code object} as a
   * {@code String} encoded using UTF-8.
   */
  public static Tap fromString(Object object) {
    return fromString(object, CharsetUtil.UTF_8);
  }

  /**
   * Create an anonymous {@code Tap} which emits the {@code String}
   * representation of an object encoded using {@code charset}.
   *
   * @param object the object to stringify and emit.
   * @param charset the {@link Charset} to use for encoding.
   * @return An anonymous {@code Tap} which will emit {@code object} as a
   * {@code String} using {@code charset}.
   */
  public static Tap fromString(Object object, Charset charset) {
    if (object == null)
      return Resources.fromSlice(null).tap();
    CharBuffer cb = CharBuffer.wrap(object.toString());
    ByteBufAllocator allo = UnpooledByteBufAllocator.DEFAULT;
    ByteBuf bb = ByteBufUtil.encodeString(allo, cb, charset);
    return Resources.fromSlice(new Slice(bb)).tap();
  }
}
