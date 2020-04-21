package socialite.tables;

import socialite.util.HashCode;
import socialite.util.SociaLiteException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Tuple_double_Object extends Tuple implements Externalizable {
  private static final long serialVersionUID = 1;

  public double _0;
  public Object _1;

  public Tuple_double_Object() {}

  public Tuple_double_Object(double __0, Object __1) {
    _0 = __0;
    _1 = __1;
  }

  public Tuple_double_Object clone() {
    return new Tuple_double_Object(_0, _1);
  }

  public int size() {
    return 2;
  }

  public void update(Tuple _t) {
    if (!(_t instanceof Tuple_double_Object))
      throw new SociaLiteException("Not supported operation");

    Tuple_double_Object t = (Tuple_double_Object) _t;
    _0 = t._0;
    _1 = t._1;
  }

  public int hashCode() {
    return HashCode.get(_0) ^ HashCode.get(_1);
  }

  public boolean equals(Object o) {
    if (!(o instanceof Tuple)) return false;

    Tuple _t = (Tuple) o;
    if (_t.getClass().equals(Tuple_double_Object.class)) {
      Tuple_double_Object t = (Tuple_double_Object) _t;
      return (_0 == (t._0)) && (_1.equals(t._1));
    }
    return false;
  }

  public Object get(int column) {
    if (column == 0) return _0;
    if (column == 1) return _1;

    return null;
  }

  public int getInt(int column) {

    throw new UnsupportedOperationException();
  }

  public long getLong(int column) {

    throw new UnsupportedOperationException();
  }

  public float getFloat(int column) {

    throw new UnsupportedOperationException();
  }

  public double getDouble(int column) {
    if (column == 0) return _0;

    throw new UnsupportedOperationException();
  }

  public Object getObject(int column) {
    if (column == 1) return _1;

    throw new UnsupportedOperationException();
  }

  public void setInt(int column, int v) {
    throw new UnsupportedOperationException();
  }

  public void setLong(int column, long v) {
    throw new UnsupportedOperationException();
  }

  public void setFloat(int column, float v) {
    throw new UnsupportedOperationException();
  }

  public void setDouble(int column, double v) {
    if (column == 0) {
      _0 = v;
      return;
    }

    throw new UnsupportedOperationException();
  }

  public void setObject(int column, Object v) {
    if (column == 1) {
      _1 = v;
      return;
    }

    throw new UnsupportedOperationException();
  }

  public String toString() {
    return "" + _0 + ", " + _1;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    _0 = in.readDouble();
    _1 = in.readObject();
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeDouble(_0);
    out.writeObject(_1);
  }
}
