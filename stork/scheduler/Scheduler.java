package stork.scheduler;

import stork.core.server.*;
import stork.util.*;
import stork.feather.*;
import stork.util.*;

import java.util.*;
import java.util.concurrent.*;

public abstract class Scheduler extends LinkedList<Job> {
  /** Schedule a job to run. */
  public boolean add(Job job) {
    try {
      schedule(job);
      return super.add(job);
    } catch (Exception e) {
      return false;
    }
  }

  /** Jobs cannot be removed. */
  public boolean remove(Object key) {
    throw new UnsupportedOperationException();
  }

  /** Jobs cannot be removed. */
  public void clear() {
    throw new UnsupportedOperationException();
  }

  /**
   * Schedule {@code job} to be executed. If {@code job} is already complete,
   * this method should silently ignore it. If {@code job} is not yet complete
   * (i.e., its status indicates that it is scheduled or running) it should be
   * stopped and rescheduled.
   */
  protected abstract void schedule(Job job);
}
