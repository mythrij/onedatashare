package stork.feather;

// A handle on the state of a data transfer.

public abstract class Transfer {
  public static TransferFrom from(Resource resource) {
    return from(resource.tap());
  } public static TransferFrom from(Tap tap) {
    return new TransferFrom(tap);
  }
}

class TransferFrom {
  Tap tap;
  public TransferFrom(Tap tap) {
    this.tap = tap;
  } public Transfer to(Resource resource) {
    return to(resource.sink());
  } public Transfer to(Sink sink) {
    // TODO
    tap.attach(sink);
    return null;
  }
}
