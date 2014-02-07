package stork.feather;

import io.netty.buffer.*;

// A slice represents a "piece" of a data resource emitted by a source and are
// Feather's fundamental message unit. Slices contain information about which
// resource they came from, their offset within the resource, and length.
//
// A slice's underlying byte buffer payload need not be a plaintext
// representation of the data (e.g., it may be encrypted or compressed), but in
// order to be accepted by a sink, it must be convertible to a representation
// the sink accepts. Ideally, for maximum compatibility, every slice
// implementation should provide a method to convert the byte payload into
// cleartext, though this is not strictly required.

public class Slice {
  public final long offset;
  protected ByteBuf bytes;

  public Slice(ByteBuf bytes, long offset) {
    this.offset = offset;
    this.bytes = bytes;
  } public Slice(ByteBuf bytes) {
    this(bytes, -1);
  } public Slice(long offset) {
    // Create an empty slice at the given offset.
    this.offset = offset;
  } public Slice() {
    // Create an empty slice with an unspecified offset.
    this(-1);
  }

  // Return a plaintext slice, or throw an OperationUnsupportedException if the
  // slice cannot be converted to plaintext. The default slice implementation
  // is assumed to be in plaintext already.
  public Slice plain() {
    return this;
  }

  // Return the length of the payload in bytes, or -1 if unknown.
  public long length() {
    return bytes.readableBytes();
  }

  // Return the byte offset of the payload relative to the 0th byte of the
  // resource, or -1 if unspecified, in which case it should be assumed to come
  // after the last slice.
  public long offset() {
    return offset;
  }

  // Return the raw byte payload.
  public final ByteBuf raw() {
    return bytes;
  }

  // An empty slice indicates the associated resource is finished transferring
  // and no further slices will be emitted for that resource.
  public boolean isEmpty() {
    return length() == 0;
  }

  // Get the resource this slice came from. Can be null if the slice is
  // anonymous.
  public Resource resource() {
    return null;
  }
}
