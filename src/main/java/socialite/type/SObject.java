package socialite.type;

public interface SObject<T> extends Comparable<T> {
  int byteSize();
}
