package socialite.dist.msg;

import java.io.Serializable;
import java.net.InetAddress;

public class CmdMessage implements Serializable {
  private static final long serialVersionUID = 1L;

  public int mMsgID;
  public static final int CONN_ACCEPTED = 0x01;
  public InetAddress[] workerAddrs;

  //
  public static final int VNODE_RUN = 0x03;
  public int mUnitRunID;
  public String mExecName;
  public InetAddress[] mUnitAddrs;

  //
  public static final int CONN_DEAD = 0x04;
  public InetAddress mDeadAddr;

  //
  public CmdMessage(int id) {
    mMsgID = id;
  }

  @Override
  public String toString() {
    switch (mMsgID) {
      case CmdMessage.CONN_ACCEPTED:
        return "CmdMessage:Connection Accepted";
      case CmdMessage.VNODE_RUN:
        return "CmdMessage:Node Run (id:" + mUnitRunID + ")";
      case CmdMessage.CONN_DEAD:
        return "CmdMessage:Connection Dead";
      default:
        return "CmdMessage:Unknown";
    }
  }
}
