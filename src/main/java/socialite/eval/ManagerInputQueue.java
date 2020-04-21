package socialite.eval;

import java.util.ArrayList;
import java.util.List;

public class ManagerInputQueue {
  List<Command> commands;
  boolean wait;

  ManagerInputQueue() {
    commands = new ArrayList<Command>();
    wait = false;
  }

  synchronized Command getCommand() throws InterruptedException {
    if (commands.isEmpty()) {
      wait = true;
      wait();
    }
    return commands.remove(0);
  }

  synchronized boolean isEmpty() {
    return commands.isEmpty();
  }

  synchronized void addCommand(Command cmd) {
    commands.add(cmd);
    if (wait) {
      wait = false;
      notify();
    }
  }
}
