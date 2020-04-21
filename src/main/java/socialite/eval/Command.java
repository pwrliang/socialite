package socialite.eval;

import java.io.Externalizable;

public interface Command extends Externalizable {
  long serialVersionUID = Command.class.hashCode();

  void setReceived();

  boolean isReceived();
}
