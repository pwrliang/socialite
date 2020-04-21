package socialite.dist.worker;

import org.apache.hadoop.io.*;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.ipc.VersionedProtocol;
import socialite.codegen.EpochW;
import socialite.resource.WorkerAddrMapW;
import socialite.tables.ConstsWritable;

public interface WorkerCmd extends VersionedProtocol {
  long versionID = 1L;

  void makeConnections(ArrayWritable otherWorkers);

  void init(WorkerAddrMapW workerAddrMapW) throws RemoteException;

  BooleanWritable isStillIdle(IntWritable epochId, IntWritable timestamp);

  void setEpochDone(IntWritable epochId);

  void haltEpoch();

  void addPyFunctions(BytesWritable classFilesBlob, BytesWritable pyfuncs);

  void addClassFiles(BytesWritable classFilesBlob);

  BooleanWritable addToClassPath(Text _path);

  void run(EpochW ew);

  BooleanWritable runQuery(
          IntWritable queryTid, Text queryClass, LongWritable iterId, ConstsWritable args);

  void cleanupTableIter(LongWritable id);

  Writable status();

  Writable status(IntWritable verbose);

  void runGc();

  void info();

  // public void setVerbose(BooleanWritable verb);
}
