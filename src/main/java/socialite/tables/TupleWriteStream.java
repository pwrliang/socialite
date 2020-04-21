package socialite.tables;

public interface TupleWriteStream {
  boolean write(Object... args);

  boolean write(Tuple t);

  void flush();

  void close();
}
