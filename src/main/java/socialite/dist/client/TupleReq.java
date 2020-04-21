package socialite.dist.client;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.ipc.VersionedProtocol;
import socialite.tables.TupleArrayWritable;

public interface TupleReq extends VersionedProtocol {
  long versionID = 1;

  BooleanWritable consume(LongWritable id, TupleArrayWritable tuples);

  void done(LongWritable id);
}
