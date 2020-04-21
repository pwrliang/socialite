package socialite.resource;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import socialite.util.SocialiteInputStream;
import socialite.util.SocialiteOutputStream;

import java.io.*;

public class WorkerAddrMapW implements Writable {
  public static final Log L = LogFactory.getLog(WorkerAddrMapW.class);

  WorkerAddrMap addrMap;

  public WorkerAddrMapW() {}

  public WorkerAddrMapW(WorkerAddrMap _addrMap) {
    addrMap = _addrMap;
  }

  public WorkerAddrMap get() {
    return addrMap;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    byte[] inBytes = new byte[size];

    in.readFully(inBytes);

    ByteArrayInputStream byteIn = new ByteArrayInputStream(inBytes);
    ObjectInputStream ois = new SocialiteInputStream(byteIn);
    try {
      addrMap = (WorkerAddrMap) ois.readObject();
    } catch (ClassNotFoundException e) {
      L.fatal("Exception while calling readFields(): " + e);
      L.fatal(ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream(256);
    ObjectOutputStream oos = new SocialiteOutputStream(byteOut); // new ObjectOutputStream(byteOut);
    oos.writeObject(addrMap);
    oos.close();
    out.writeInt(byteOut.size());
    out.write(byteOut.toByteArray());
  }
}
