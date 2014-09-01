package stork.scheduler;

import stork.core.server.*;
import stork.util.*;
import stork.feather.*;
import stork.util.*;

import java.util.*;
import java.util.concurrent.*;

public abstract class Scheduler implements Collection<Job> {
  private int size = 0;

  /**
   * Schedule {@code job} to be executed. If {@code job} is already complete,
   * this method should silently ignore it. If {@code job} is not yet complete
   * (i.e., its status indicates that it is scheduled or running) it should be
   * stopped and rescheduled.
   */
  protected abstract void schedule(Job job);

  /** Get the job with the given {@code id}. */
  public abstract Job get(int id);

  /** Schedule a job to run. */
  public final boolean add(Job job) {
    try {
      schedule(job);
      size++;
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }

  public final boolean addAll(Collection<? extends Job> jobs) {
    boolean changed = false;
    for (Job job : jobs)
      changed = add(job) || changed;
    return changed;
  }

  /** Jobs cannot be removed. */
  public void clear() {
    throw new UnsupportedOperationException();
  }

  public boolean contains(Object o) {
    if (o == null || !(o instanceof Job))
      return false;
    Job job = (Job) o;
    if (job.jobId() <= 0)
      return false;
    Job job2 = get(job.jobId());
    if (job2 == null)
      return false;
    return job.equals(job2);
  }

  public boolean containsAll(Collection<?> c) {
    for (Object o : c)
      if (!contains(c)) return false;
    return true;
  }

  public boolean equals(Object o) {
    if (!(o instanceof Scheduler))
      return false;
    return containsAll((Scheduler) o);
  }

  public int hashCode() {
    int code = 0;
    for (Job j : this)
      code += j.hashCode();
    return code;
  }

  public boolean isEmpty() {
    return size() <= 0;
  }

  public final Iterator<Job> iterator() {
    return new Iterator<Job>() {
      int index = 0;
      public boolean hasNext() {
        return index < size;
      } public Job next() {
        return get(index++);
      } public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  public int size() { return size; }

  public final Object[] toArray() {
    return toArray(new Job[size]);
  }

  public final <T> T[] toArray(T[] a) {
    Class<T> c = (Class<T>) a.getClass().getComponentType();
    if (a.length < size) {
      a = Arrays.copyOf(a, size);
    } else if (a.length > size) {
      Arrays.fill(a, size, a.length, null);
    } for (int i = 0; i < size; i++) {
      a[i] = c.cast(get(i));
    } return a;
  }
}
