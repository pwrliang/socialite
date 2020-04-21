package socialite.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.eval.TmpTablePool;
import socialite.tables.TmpTableInst;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;

public class FastInputStream extends ObjectInputStream {
  public static final Log L = LogFactory.getLog(FastInputStream.class);

  InputStream in;
  FastClassLookup lookup;

  public FastInputStream(InputStream _in) throws IOException {
    super();
    in = _in;
    lookup = new FastClassLookup();
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public int read() throws IOException {
    return in.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return in.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return in.read(b, off, len);
  }

  @Override
  public Object readObjectOverride() throws IOException, ClassNotFoundException {
    int classNameLen = readInt();
    final char[] _className = new char[classNameLen];
    for (int i = 0; i < _className.length; i++) {
      _className[i] = readChar();
    }
    String className = new String(_className);
    if (className.startsWith("[")) {
      return readArray(className);
    }
    Class<?> cls = getClass(className);
    if (cls.equals(String.class)) {
      char[] s = new char[readInt()];
      for (int i = 0; i < s.length; i++) s[i] = readChar();
      return new String(s);
    }
    Object obj = null;
    try {
      if (TmpTableInst.class.isAssignableFrom(cls)) {
        char size = readChar();
        if (size == 2) obj = TmpTablePool._get(cls);
        else if (size == 1) obj = TmpTablePool._getSmall(cls);
        else new AssertionError("Unexpected size for " + cls.getName());
      } else {
        obj = cls.newInstance();
      }
      assert obj instanceof Externalizable;
      Externalizable ext = (Externalizable) obj;
      ext.readExternal(this);
      return obj;
    } catch (Exception e) {
      L.fatal("Exception while creating table instance:" + e);
      L.fatal(ExceptionUtils.getStackTrace(e));
      throw new SociaLiteException(e);
    }
  }

  static TmpTableInst newInstance(Class<?> tableCls) throws Exception {
    Constructor<?> c = tableCls.getDeclaredConstructor((Class[]) null);
    c.setAccessible(true);
    TmpTableInst i = (TmpTableInst) c.newInstance((Object[]) null);
    return i;
  }

  private Class<?> getClass(String className) {
    if (className.charAt(0) == '#') {
      int fastIdx = Integer.parseInt(className.substring(1));
      return lookup.getClass(fastIdx);
    } else {
      Class<?> cls = Loader.forName(className);
      lookup.addClass(cls);
      return cls;
    }
  }

  Object readArray(String className) throws IOException, ClassNotFoundException {
    className = className.substring(1);
    Class<?> cls = getClass(className);
    if (cls.isPrimitive()) {
      return readPrimArray(cls);
    } else {
      return readObjectArray(cls);
    }
  }

  Object readPrimArray(Class<?> cls) throws IOException, ClassNotFoundException {
    int arraylen = readInt();
    Object ret = null;
    if (cls.equals(int.class)) {
      int[] array = new int[arraylen];
      for (int i = 0; i < array.length; i++) {
        array[i] = readInt();
      }
      ret = array;
    } else if (cls.equals(long.class)) {
      long[] array = new long[arraylen];
      for (int i = 0; i < array.length; i++) {
        array[i] = readLong();
      }
      ret = array;
    } else if (cls.equals(float.class)) {
      float[] array = new float[arraylen];
      for (int i = 0; i < array.length; i++) {
        array[i] = readFloat();
      }
      ret = array;
    } else if (cls.equals(double.class)) {
      double[] array = new double[arraylen];
      for (int i = 0; i < array.length; i++) {
        array[i] = readDouble();
      }
      ret = array;
    } else if (cls.equals(byte.class)) {
      byte[] array = new byte[arraylen];
      for (int i = 0; i < array.length; i++) {
        array[i] = readByte();
      }
      ret = array;
    } else if (cls.equals(boolean.class)) {
      boolean[] array = new boolean[arraylen];
      for (int i = 0; i < array.length; i++) {
        array[i] = readBoolean();
      }
      ret = array;
    } else if (cls.equals(short.class)) {
      short[] array = new short[arraylen];
      for (int i = 0; i < array.length; i++) {
        array[i] = readShort();
      }
      ret = array;
    } else if (cls.equals(char.class)) {
      char[] array = new char[arraylen];
      for (int i = 0; i < array.length; i++) {
        array[i] = readChar();
      }
      ret = array;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported type array:" + cls.getSimpleName() + "[");
    }
    return ret;
  }

  Object readObjectArray(Class<?> cls) throws IOException, ClassNotFoundException {
    int arrayLen = readInt();
    Object[] array = (Object[]) Array.newInstance(cls, arrayLen);

    for (int i = 0; i < array.length; i++) {
      if (readBoolean()) {
        try {
          if (cls.equals(String.class)) {
            int slen = readInt();
            final char[] chars = new char[slen];
            for (int j = 0; j < slen; j++) chars[j] = readChar();
            String s = new String(chars);
            array[i] = s;
            continue;
          }

          if (TmpTableInst.class.isAssignableFrom(cls)) {
            char size = readChar();
            if (size == 2) array[i] = TmpTablePool._get(cls);
            else if (size == 1) array[i] = TmpTablePool._getSmall(cls);
            else new AssertionError("Unexpected size for " + cls.getName());

            ((Externalizable) array[i]).readExternal(this);
          } else {
            array[i] = cls.newInstance();
            ((Externalizable) array[i]).readExternal(this);
          }

        } catch (Exception e) {
          L.fatal("Exception while creating table instance:" + e);
          L.fatal(ExceptionUtils.getStackTrace(e));
          throw new SociaLiteException(e);
        }
      } else {
        array[i] = null;
      }
    }
    return array;
  }

  @Override
  public boolean readBoolean() throws IOException {
	  return in.read() == 1;
  }

  @Override
  public byte readByte() throws IOException {
    return (byte) in.read();
  }

  @Override
  public char readChar() throws IOException {
    char a = (char) readByte();
    char b = (char) readByte();
    return (char) ((a << 8) | (b & 0xff));
  }

  @Override
  public double readDouble() throws IOException {
    long l = readLong();
    return Double.longBitsToDouble(l);
  }

  @Override
  public float readFloat() throws IOException {
    int i = readInt();
    return Float.intBitsToFloat(i);
  }

  @Override
  public int readInt() throws IOException {
    int a = readByte();
    int b = readByte();
    int c = readByte();
    int d = readByte();
    return (((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff));
  }

  @Override
  public long readLong() throws IOException {
    long a = readByte();
    long b = readByte();
    long c = readByte();
    long d = readByte();
    long e = readByte();
    long f = readByte();
    long g = readByte();
    long h = readByte();
    return (((a & 0xff) << 56)
        | ((b & 0xff) << 48)
        | ((c & 0xff) << 40)
        | ((d & 0xff) << 32)
        | ((e & 0xff) << 24)
        | ((f & 0xff) << 16)
        | ((g & 0xff) << 8)
        | h & 0xff);
  }

  @Override
  public short readShort() throws IOException {
    short a = readByte();
    short b = readByte();
    return (short) ((a << 8) | (b & 0xff));
  }

  @Override
  public int readUnsignedShort() throws IOException {
    short a = readByte();
    short b = readByte();
    return (((a & 0xff) << 8) | (b & 0xff));
  }

  @Override
  public int readUnsignedByte() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public String readUTF() throws IOException {
    throw new UnsupportedOperationException();
  }
}
