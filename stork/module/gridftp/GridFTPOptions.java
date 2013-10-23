package stork.module.gridftp;

import stork.module.*;

public class GridFTPOptions implements TransferOptions {
  public int parallelism   = 4;
  public int concurrency   = 2;
  public int pipelining    = 20;
  public boolean overwrite = false;
  public boolean verify    = false;
  public boolean encrypt   = false;
  public boolean compress  = false;
}
