package socialite.tables;

import java.util.Iterator;

public interface ConcurrentTableInst extends TableInst {
  void insertAtomic(Iterator<Tuple> iterator);
}
