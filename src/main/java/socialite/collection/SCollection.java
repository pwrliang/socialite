package socialite.collection;

import socialite.tables.Tuple;

public interface SCollection {
  boolean isEmpty();

  void clear();

  boolean add(Tuple t);

  boolean add(int i);

  boolean add(long l);

  boolean add(float f);

  boolean add(double d);

  boolean remove(Tuple t);

  boolean remove(int i);

  boolean remove(long l);

  boolean remove(float f);

  boolean remove(double d);

  boolean contains(Tuple t);

  boolean contains(int i);

  boolean contains(long l);

  boolean contains(float f);

  boolean contains(double d);

  boolean isSorted();

  boolean isSortedAsc();

  boolean isSortedDesc();

  boolean removeLast();

  boolean removeFirst();

  void iterate(TupleVisitor v);
}
