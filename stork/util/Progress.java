package stork.util;

// Class representing progress for an arbitrary statistic.

public class Progress {
  public long done = 0, total = 0;

  public Progress() { }

  public Progress(long total) {
    this.total = total;
  }

  public long remaining() {
    return total-done;
  }

  // Reset this progress tracker and set the total.
  public void reset(long t) {
    done = 0;
    total = t;
  }

  // Finish this progress automatically, e.g. when we know a transfer has
  // completed successfully and we don't want to fill the progress the rest
  // of the way.
  public void finish() {
    if (done < total)
      add(total-done, 0);
    else
      add(0, 0);
  }

  // Everything should call this so subclasses can just hook this guy.
  public void add(long d, long t) {
    done += d; total += t;
    if (done > total) done = total;
  }

  public void add(Progress p) {
    add(p.done, p.total);
  }

  public void add(long d) {
    add(d, 0);
  }

  public String toPercent() {
    if (total <= 0)
      return "0.00%";
    return String.format("%.0f%%", 100.0 * done / total);
  }

  public String toString() {
    if (total <= 0)
      return null;
    return done+"/"+total;
  }
}
