package socialite.collection;

import socialite.tables.Tuple;

public interface TupleVisitor {
  boolean visit(int i);

  boolean visit(long l);

  boolean visit(float f);

  boolean visit(double d);

  boolean visit(Object o);

  boolean visit(Tuple t);
}
