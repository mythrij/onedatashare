package stork.scheduler;

import stork.util.*;
import java.util.*;

// States a job can be in, and some special status filters.

public enum JobStatus {
  scheduled, processing, paused, removed, failed, complete,
  all(false), pending(false), done(false);

  public final boolean isFilter;  // Jobs cannot have filter statuses.

  JobStatus() {
    this(true);
    //filter = EnumSet.of(Enum.valueOf(S.class, name()));
  } JobStatus(boolean real) {
    isFilter = !real;
  }

  public static JobStatus unmarshal(String s) {
    return byName(s);
  }

  // Get a filter for a status.
  public static EnumSet<JobStatus> filter(String s) {
    return byName(s).filter();
  } public EnumSet<JobStatus> filter() {
    if (!isFilter) {
      return EnumSet.of(this);
    } switch (this) {
      case all:
        return EnumSet.of(scheduled, processing, paused,
                          removed, failed, complete);
      case pending:
        return EnumSet.of(scheduled, processing, paused);
      case done:
        return EnumSet.of(removed, failed, complete);
      default: return null;
    }
  }

  // Get a status by name. 
  public static JobStatus byName(String name) {
    if (name == null)
      return null;
    return Enum.valueOf(JobStatus.class, name.toLowerCase());
  }
}
