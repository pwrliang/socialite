package socialite.eval;

import socialite.tables.Joiner;
import socialite.tables.TableInst;

public class EvalTask implements Task {
  Joiner visitor;
  // TableInst deltaT;
  int priority;

  public EvalTask(Joiner _v) {
    visitor = _v;
  }

  public void setPriority(int _priority) {
    priority = _priority;
  }

  public int getPriority() {
    return priority;
  }

  @Override
  public void run(Worker w) {
    visitor.setWorker(w);
    visitor.run();
  }

  public TableInst[] getResultDeltaTable() {
    return visitor.getDeltaTables();
  }

  public int getEpochId() {
    return visitor.getEpochId();
  }

  public int getRuleId() {
    return visitor.getRuleId();
  }

  public String toString() {
    return visitor.toString();
  }
}
