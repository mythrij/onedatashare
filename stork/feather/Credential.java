package stork.feather;

// An abstract credential.

public abstract class Credential<O> {
  // Get the raw credential object.
  public abstract O data();

  // Get the lifetime of the credential in milliseconds, or 0 if the credential
  // will never expire.
  public long duration() {
    return 0;
  }
}
