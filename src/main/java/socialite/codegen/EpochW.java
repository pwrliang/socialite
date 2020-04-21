package socialite.codegen;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import socialite.util.SociaLiteException;
import socialite.util.SocialiteInputStream;
import socialite.util.SocialiteOutputStream;

import java.io.*;

public class EpochW implements Writable {
  public static final Log L = LogFactory.getLog(EpochW.class);

  Epoch e;
  byte[] data;

  public EpochW() {}

  public EpochW(Epoch _e) {
    e = _e;
    try {
      ByteArrayOutputStream byteOut = new ByteArrayOutputStream(12 * 1024);
      ObjectOutputStream oos = new SocialiteOutputStream(byteOut);
      oos.writeObject(e);
      oos.close();
      data = byteOut.toByteArray();
      // L.info("EpochW size:"+data.length);
    } catch (Exception e) {
      L.error("Exception during serialization:" + ExceptionUtils.getStackTrace(e));
      throw new SociaLiteException(e);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    byte[] inBytes = new byte[size];

    try {
      in.readFully(inBytes);
      ByteArrayInputStream byteIn = new ByteArrayInputStream(inBytes);
      ObjectInputStream ois = new SocialiteInputStream(byteIn);
      try {
        e = (Epoch) ois.readObject();
      } catch (ClassNotFoundException e) {
        L.fatal("readFields():" + ExceptionUtils.getStackTrace(e));
      }
      ois.close();
    } catch (Exception e) {
      L.error("readFields():" + ExceptionUtils.getStackTrace(e));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(data.length);
    out.write(data);
  }

  public Epoch get() {
    return e;
  }
}
