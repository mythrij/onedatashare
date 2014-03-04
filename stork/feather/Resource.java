package stork.feather;

/**
 * A handle on a remote resource, such as a file or directory. A resource
 * should essentially be a wrapper around a URI, and its creation should have
 * no side-effects. All of the methods specified by this interface should
 * return immediately. In the case of methods that return {@link Bell}s, the
 * result should be determined asynchronously and given to the caller through
 * the returned bell. Implementations that do not support certain operations
 * are allowed to throw an {@link UnsupportedOperationException}.
 *
 * @see Session
 * @see Stat
 * @see URI
 */
public interface Resource {
  /**
   * Get the {@link Session) associated with this resource.
   *
   * @return The {@link Session} associated with this resource.
   * @see Session
   */
  Session session();

  /**
   * Get the {@link URI} associated with this resource.
   * 
   * @return A {@link URI} which identifies this resource.
   */
  URI uri();

  /**
   * Get metadata for this resource, which includes a list of subresources.
   *
   * @return (via bell) A {@link Stat} containing resource metadata.
   * @throws ResourceException (via bell) if there was an error retrieving
   * metadata for the resource
   * @throws UnsupportedOperationException if metadata retrieval is not
   * supported
   */
  Bell<Stat> stat();

  /**
   * Create this resource as a directory on the storage system. If the resource
   * cannot be created, or already exists and is not a directory, the returned
   * {@link Bell} will be resolved with a {@link ResourceException}.
   *
   * @return (via bell) {@code null} if successful.
   * @throws ResourceException (via bell) if the directory could not be created
   * or already exists and is not a directory
   * @throws UnsupportedOperationException if creating directories is not
   * supported
   * @see Bell
   */
  Bell<Void> mkdir();

  /**
   * Delete the resource and all subresources from the storage system. If the
   * resource cannot be removed, the returned {@link Bell} will be resolved
   * with a ResourceException.
   *
   * @return (via bell) {@code null} if successful.
   * @throws ResourceException (via bell) if the resource could not be fully
   * removed
   * @throws UnsupportedOperationException if removal is not supported
   * @see Bell
   */
  Bell<Void> rm();

  /**
   * Select a subresource relative to this resource.
   */
  //Resource select(Path path);

  /**
   * Called by client code to initiate a transfer to the named {@link Resource}
   * using whatever method is deemed most appropriate by the implementation.
   * The implementation should try to transfer the resource as efficiently as
   * possible, as so should inspect the destination resource to determine if
   * more efficient alternatives to proxy transferring can be done. This method
   * should perform a proxy transfer as a catch-all last resort.
   *
   * @param resource the destination resource to transfer this resource to
   * @return (via bell) A {@link Transfer} on success; the returned {@code
   * Transfer} object can be used to control and monitor the transfer.
   * @throws ResourceException (via bell) if the transfer fails
   * @throws UnsupportedOperationException if the direction of transfer is not
   * supported by one of the resources
   * @see Bell
   */
  Bell<Transfer> transferTo(Resource resource);

  /**
   * Open a sink to the resource. Any connection operation, if necessary,
   * should begin as soon as this method is called. The returned bell should
   * be rung once the sink is ready to accept data.
   *
   * @return (via bell) A sink which drains to the named resource.
   * @throws ResourceException (via bell) if opening the sink fails
   * @throws UnsupportedOperationException if the resource does not support
   * writing
   * @see Bell
   */
  Bell<Sink> sink();

  /**
   * Open a tap on the resource. Any connection operation, if necessary, should
   * begin, as soon as this method is called. The returned bell should be rung
   * once the tap is ready to emit data.
   *
   * @return (via bell) A tap which emits slices from this resource and its
   * subresources.
   * @throws ResourceException (via bell) if opening the tap fails
   * @throws UnsupportedOperationException if the resource does not support
   * reading
   * @see Bell
   */
  Bell<Tap> tap();
}
