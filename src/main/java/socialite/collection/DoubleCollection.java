package socialite.collection;

public interface DoubleCollection {
  boolean add(double d);

  void clear();

  boolean contains(double d);

  boolean isEmpty();

  int size();

  DoubleIterator iterator();
}
