package stork.feather;

/** Interface for transfer elements that provide methods for flow control. */
public interface Controller {
  /**
   * Start the flow of data.
   *
   * @return A {@code Bell} which will ring once the flow of data begins.
   */
  Bell<?> start();

  /** Stop the flow of data permanently. */
  void stop();

  /**
   * Pause the transfer. The returned {@code Bell} rings on resume.
   *
   * @return A {@code Bell} which will ring when the flow resumes, or can be
   * rung externally to resume the flow.
   */
  Bell<?> pause();

  /** Resume the the flow of data after a pause. */
  void resume();
}
