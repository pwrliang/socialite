package socialite.tables;

/** A SociaLite table exposed to end-users */
public interface TableRef {
  TupleWriteStream getWriteStream();
}
