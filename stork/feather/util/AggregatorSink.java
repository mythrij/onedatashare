package stork.feather.util;

import io.netty.buffer.*;

import stork.feather.*;

/**
 * A {@code Sink} which aggregates multiple, randomly-ordered {@code Slices}
 * into a single {@code Slice} which it rings a bell with on finalization.
 */
public class AggregatorSink extends Sink {
  private Bell<Slice> bell = new Bell<Slice>();
  private List<ByteBuf> list = new LinkedList<ByteBuf>();

  public AggregatorSink() {
    super(Resources.ANONYMOUS);
  }

  public void drain(RelativeSlice slice) {
    if (slice.path().isRoot())
      list.add(slice);
  }

  public void finalize(RelativeResource resource) {
    if (resource.isRoot()) {
      ByteBuf[] array = list.asArray(new ByteBuf[0]);
      ByteBuf buf = Unpooled.wrappedBuffer(array);
      bell.ring(new Slice(buf));
    }
  }

  public Bell<ByteBuf> bell() {
    return bell;
  }

  //public boolean random() { return true; }
}
