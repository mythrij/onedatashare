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
      return null;
    return String.format("%.2f%%", 100.0 * done / total);
  }

  public String toString() {
    if (total <= 0)
      return null;
    return done+"/"+total;
  }
}
