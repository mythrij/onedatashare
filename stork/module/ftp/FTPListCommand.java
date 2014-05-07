package stork.module.ftp;

/**
 * FTP listing commands in order of priority.
 */
public enum FTPListCommand {
  MLSC, STAT, MLSD, LIST(true), NLST(true);

  private boolean dataChannel;

  private FTPListCommand() {
    this(false);
  }

  private FTPListCommand(boolean dataChannel) {
    this.dataChannel = dataChannel;
  }

  /**
   * Get the listing command that should be used after this one. Or {@code
   * null} if this is the least preferable one.
   */
  public FTPListCommand next() {
    final int n = ordinal()+1;
    return (n < values().length) ? values()[n] : null;
  }

  /**
   * Check if this listing command requires a data channel.
   */
  public boolean requiresDataChannel() {
    return dataChannel;
  }
}
