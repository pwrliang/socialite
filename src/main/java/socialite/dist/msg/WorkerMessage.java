package socialite.dist.msg;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.eval.Command;
import socialite.eval.EvalWithTable;
import socialite.eval.TmpTablePool;
import socialite.tables.TableInst;
import socialite.tables.TmpTableInst;
import socialite.util.ByteBufferOutputStream;
import socialite.util.FastOutputStream;

import java.io.*;
import java.nio.ByteBuffer;

public class WorkerMessage implements Externalizable {
  static final long serialVersionUID = 1;
  public static final Log L = LogFactory.getLog(WorkerMessage.class);

  public EvalWithTable evalT;
  transient int epochId;
  transient int workerid;
  transient ByteBuffer serialized;
  transient int tableid = -1;

  public WorkerMessage() {}

  public WorkerMessage(EvalWithTable _cmd) {
    evalT = _cmd;
    epochId = evalT.getEpochId();
  }

  public int getEpochId() {
    return epochId;
  }

  public int tableId() {
    return evalT.getTable().id();
  }

  public boolean isSerialized() {
    return serialized != null;
  }

  public ByteBuffer serialize(boolean free) throws IOException {
    if (serialized == null) {
      final TmpTableInst t = evalT.getTable();
      tableid = t.id();
      int size = guessMessageSize();
      ByteBufferOutputStream bbos = new ByteBufferOutputStream(size);
      final float incby = 2f;
      bbos.onCapacityInc(
          incby,
          new Runnable() {
            public void run() {
              t.incEstimFactor(incby);
              t.setReuse(false);
            }
          });
      ObjectOutputStream oos = new FastOutputStream(bbos);
      oos.writeObject(this);
      oos.flush();
      oos.close();
      t.setSharedSizeEstimFactor(t.sizeEstimFactor());
      serialized = bbos.buffer();
      if (free) {
        TmpTablePool.free(t);
      }
      evalT = null;
    }
    return serialized;
  }

  public ByteBuffer serialize() throws IOException {
    return serialize(true);
  }

  public int guessMessageSize() {
    int size = evalT.getTable().totalDataSize();
    return size;
  }

  public void emptyTable() {
    getTable().clear();
  }

  public void setWorkerId(int _workerid) {
    workerid = _workerid;
  }

  public int getWorkerId() {
    return workerid;
  }

  public TableInst getTable() {
    return evalT.getTable();
  }

  public int getTableId() {
    if (tableid >= 0) return tableid;
    return evalT.getTable().id();
  }

  public Command get() {
    return evalT;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    evalT = (EvalWithTable) in.readObject();
    epochId = evalT.getEpochId();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(evalT);
  }

  public String toString() {
    return evalT.toString();
  }
}
