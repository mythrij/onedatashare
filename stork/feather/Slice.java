package stork.feather;

import io.netty.buffer.*;
import io.netty.util.*;

/**
 * A slice represents a "piece" of a data resource emitted by a tap and are
 * Feather's fundamental message unit. Slices contain information about which
 * resource they came from, their offset within the resource, and length.
 *
 * A slice's underlying byte buffer payload need not be a plaintext
 * representation of the data (e.g., it may be encrypted or compressed), but in
 * order to be accepted by a sink, it must be convertible to a representation
 * the sink accepts. Ideally, for maximum compatibility, every slice
 * implementation should provide a method to convert the byte payload into
 * cleartext, though this is not strictly required.
 */
public class Slice {
  /** The offset of the slice in its resource of origin. */
  public final long offset;

  /** The raw payload buffer. */
  protected ByteBuf bytes;

  /**
   * Create a slice wrapping the given bytes, from the given offset.
   *
   * @param bytes the bytes to wrap
   * @param offset the offset the bytes came from
   */
  public Slice(ByteBuf bytes, long offset) {
    this.offset = offset;
    this.bytes = bytes;
  }

  /**
   * Create a slice wrapping the given bytes.
   *
   * @param bytes the bytes to wrap
   */
  public Slice(ByteBuf bytes) {
    this(bytes, -1);
  }

  /**
   * Create an empty slice from the given offset.
   *
   * @param offset the offset the bytes came from
   */
  public Slice(long offset) {
    // Create an empty slice at the given offset.
    this(Unpooled.EMPTY_BUFFER, offset);
  }

  /**
   * Create an empty slice.
   */
  public Slice() {
    // Create an empty slice with an unspecified offset.
    this(Unpooled.EMPTY_BUFFER, -1);
  }

  /**
   * Return a plaintext slice, or throw {@code OperationUnsupportedException}
   * if the slice cannot be converted to plaintext. The default slice
   * implementation is assumed to be in plaintext already.
   *
   * @throws OperationUnsupportedException if the slice cannot be converted to
   * plaintext
   * @return A plaintext slice. The default implementation assumes it is
   * already in a plaintext format.
   */
  public Slice plain() {
    return this;
  }

  /**
   * Return the length of the payload in bytes, or -1 if unknown. Specifically,
   * this is the length of the plaintext bytes represented by this slice, which
   * may be different than the raw size.
   *
   * @return The plaintext length in bytes.
   */
  public long length() {
    return bytes.readableBytes();
  }

  /**
   * Return the byte offset of the plaintext payload relative to the 0th byte
   * of the resource, or {@code -1} if unspecified, in which case it should be
   * assumed to come after the last slice.
   *
   * @return The offset of the plaintext slice in the resource of origin.
   */
  public long offset() {
    return offset;
  }

  /**
   * Return the raw byte payload encapsulated by this slice.
   *
   * @return The raw data as a Netty {@link ByteBuf}.
   */
  public final ByteBuf raw() {
    return bytes;
  }

  /**
   * Return the raw data wrapped by the slice as a byte array.
   *
   * @return The raw data as a byte array.
   */
  public final byte[] asBytes() {
    ByteBuf raw = raw();
    byte[] bytes;
    if (raw.hasArray())
      bytes = raw.array();
    else
      raw.getBytes(0, bytes = new byte[raw.readableBytes()]);
    return bytes;
  }

  /**
   * Check if the slice is empty, that is its payload length is 0. An empty
   * slice indicates the associated resource is finished transferring and no
   * further slices will be emitted for that resource.
   *
   * @return {@code true} if this slice is empty; {@code false} otherwise.
   */
  public boolean isEmpty() {
    return length() == 0;
  }

  /**
   * Get the {@link Resource} this slice came from. Can be {@code null} if the slice is
   * anonymous.
   *
   * @return The resource this slice came from, or {@code null}.
   * @see Resource
   */
  public Resource resource() {
    return null;
  }

  /**
   * Return a string representation of the slice that can be useful for
   * debugging.
   *
   * @return A string representation describing this slice.
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[\n")
    .append("  utf_8 = \"")
      .append(raw().toString(CharsetUtil.UTF_8))
    .append("\"\n")
    .append("]");
    return sb.toString();
  }
}
