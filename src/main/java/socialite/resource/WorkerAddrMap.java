package socialite.resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import socialite.dist.PortMap;
import socialite.util.UnresolvedSocketAddr;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class WorkerAddrMap implements Serializable {
  static final Log L = LogFactory.getLog(WorkerAddrMap.class);

  int lastUnusedIndex;
  ArrayList<UnresolvedSocketAddr> workerAddrs;
  ArrayList<UnresolvedSocketAddr> workerDataAddrs;
  transient int myIdx = -1;

  public WorkerAddrMap() {
    lastUnusedIndex = 0;
    workerAddrs = new ArrayList<UnresolvedSocketAddr>();
    workerDataAddrs = new ArrayList<UnresolvedSocketAddr>();
    myIdx = -1;
  }

  public void add(UnresolvedSocketAddr addr, UnresolvedSocketAddr dataAddr) {
    workerAddrs.add(addr);
    workerDataAddrs.add(dataAddr);
  }

  public UnresolvedSocketAddr getDataAddr(int workerIdx) {
    return workerDataAddrs.get(workerIdx);
  }

  public UnresolvedSocketAddr get(int workerIdx) {
    return workerAddrs.get(workerIdx);
  }

  public int size() {
    return workerAddrs.size();
  }

  public void initMyIndex() {
    assert myIdx == -1;
    String host = NetUtils.getHostname().split("/")[1];
    int port = PortMap.worker().getPort("workerCmd");
    UnresolvedSocketAddr myaddr = new UnresolvedSocketAddr(host, port);
    myIdx = 0;
    for (UnresolvedSocketAddr addr : workerAddrs) {
      if (myaddr.equals(addr)) {
        return;
      }
      myIdx++;
    }

    throw new AssertionError("Cannot find worker index (host=" + host + ", port=" + port + ")");
  }

  public int myIndex() {
    return myIdx;
  }

  public String toString() {
    String str = "";
    for (UnresolvedSocketAddr addr : workerAddrs) {
      str += addr + ", ";
    }
    return str;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    myIdx = -1;
  }
}
