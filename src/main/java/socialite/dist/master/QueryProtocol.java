package socialite.dist.master;

import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface QueryProtocol extends VersionedProtocol {
  long versionID = 1L;

  void addPyFunctions(BytesWritable classFilesBlob, BytesWritable bytesPyfuncs)
      throws RemoteException;

  void addClassFiles(BytesWritable classFilesBlob) throws RemoteException;

  void run(Text program) throws RemoteException;

  void run(Text program, Text clientIp, IntWritable port) throws RemoteException;

  void run(Text prog, Text clientIp, IntWritable port, LongWritable id)
      throws RemoteException;

  void cleanupTableIter(LongWritable id);

  BytesWritable status();

  BytesWritable status(IntWritable verbose);

  BooleanWritable addToClassPath(Text hdfsPath);

  BooleanWritable removeFromClassPath(Text hdfsPath);

  // for debugging
  void runGc();

  void info();

  void setVerbose(BooleanWritable verbose);
}
