package stork.feather.util;

/**
 * This is used to track the progress of a transfer in real time.
 */
public class TransferInfo {
  /** Units complete. */
  public long done;
  /** Total units. */
  public long total;
  /** Average throughput. */
  public double avg;
  /** Instantaneous throughput. */
  public double inst;

  /** Update based on the given information. */
  public void update(Time time, Progress p, Throughput tp) {
    done = p.done();
    total = p.total();
    avg = p.rate(time).value();
    inst = tp.value();
  }
}
