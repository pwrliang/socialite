package socialite.tables;

import java.io.Externalizable;

// For storing data to transfer to remote machines,
// or for storing deltas.
public abstract class TmpTableInst extends AbstractTableInst implements Externalizable {
  public abstract int ordinaryCapacity();

  public abstract int capacity();

  public abstract int size();

  public abstract int vacancy(); // capacity - current size

  public abstract boolean isSmall(); // has small # of elems compared to ordinaryCapacity()

  public abstract void addAll(TmpTableInst table);

  boolean _reuse = true;

  public void setReuse(boolean reuse) {
    _reuse = reuse;
  }

  public boolean reuse() {
    return _reuse;
  }

  // if the table partition map returns 0,
  // JoinerBuilder uses the following method to decide
  // the # of slices and the # of visitors
  public int partitionNum() {
    return 1;
  }

  // Estimates the size of data in the table to copy to network buffer
  public abstract int totalDataSize(); // total size of data in this table instance.

  float _estim = -1;

  public void incEstimFactor(float incby) {
    _estim = _estim * incby;
  }

  public float sizeEstimFactor() {
    if (_estim < 0) _estim = sharedSizeEstimFactor();
    return _estim;
  }

  public abstract void setSharedSizeEstimFactor(float estimFactor);

  public abstract float sharedSizeEstimFactor();

  public void enableInternalLock(boolean writeOnly) {
    // do nothing
  }

  public void disableInternalLock() {
    // do nothing
  }

  public LockStatus isLockEnabled() {
    return LockStatus.disabled;
  }

  public void iterate_at(ColumnConstraints constr, int offset, Object v) {
    // do nothing
  }
}
