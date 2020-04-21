package socialite.tables;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import socialite.util.SocialiteInputStream;
import socialite.util.SocialiteOutputStream;

import java.io.*;

public class TupleW implements Writable {
  public static final Log L = LogFactory.getLog(TupleW.class);

  Tuple t;

  public TupleW() {}

  public TupleW(Tuple _t) {
    t = _t;
  }

  public Tuple get() {
    return t;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    byte[] inBytes = new byte[size];

    in.readFully(inBytes);

    ByteArrayInputStream byteIn = new ByteArrayInputStream(inBytes);
    ObjectInputStream ois = new SocialiteInputStream(byteIn);
    try {
      t = (Tuple) ois.readObject();
    } catch (ClassNotFoundException e) {
      L.fatal("ClassNotFound while writable.readFields:" + e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ByteArrayOutputStream byteOut = new ByteArrayOutputStream(256);
    ObjectOutputStream oos = new SocialiteOutputStream(byteOut); // new ObjectOutputStream(byteOut);
    oos.writeObject(t);
    oos.close();
    out.writeInt(byteOut.size());
    out.write(byteOut.toByteArray());
  }
}
