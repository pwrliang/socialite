package socialite.eval;

public interface Task {
  void setPriority(int priority);

  int getPriority();

  void run(Worker w);

  int getRuleId();

  int getEpochId();
}
