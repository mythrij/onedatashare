package stork.feather;

// A handle on a remote resource, such as a file or directory.

public abstract class Resource {
  public final URI uri;

  protected Bell<Stat> stat;

  // Create a resource identified by the given session and URI.
  protected Resource(URI uri) {
    this.uri = uri;
  }

  // Get the session associated with a resource.
  public abstract <S extends Session> S session();

  // Called by client code to initiate a transfer using whatever method is
  // deemed most appropriate by the session implementation. Subclasses may want
  // to override this to support cases where server-to-server or local
  // transfers are possible. By default, this will simply perform a proxy
  // transfer through the local connection.
  public Transfer transferTo(Resource r) {
    return Transfer.from(tap()).to(r.sink());
  }

  // Open a sink to the resource.
  public abstract Sink sink();

  // Open a tap on the resource.
  public abstract Tap tap();
}
