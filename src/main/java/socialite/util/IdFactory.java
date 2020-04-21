package socialite.util;

public class IdFactory {
  static int nextTableId = 0;
  static int nextEpochId = 0;
  static int nextRuleId = 0;
  static int nextVarId = 0;
  static int pyfuncId = 0;

  public static void reset() {
    nextTableId = 0;
    nextEpochId = 0;
    nextRuleId = 0;
    nextVarId = 0;
    pyfuncId = 0;
  }

  public static synchronized int nextTableId() {
    return nextTableId++;
  }

  public static synchronized void tableIdAdvanceTo(int id) {
    if (id > nextTableId) nextTableId = id;
  }

  public static synchronized int nextEpochId() {
    return nextEpochId++;
  }

  public static synchronized int nextRuleId() {
    return nextRuleId++;
  }

  public static synchronized int nextVarId() {
    return nextVarId++;
  }

  public static synchronized int nextPyFuncId() {
    return pyfuncId++;
  }
}
