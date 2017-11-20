package socialite.async;

import mpi.MPI;
import mpi.MPIException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.dist.master.AsyncMaster;
import socialite.async.dist.worker.AsyncWorker;
import socialite.async.engine.LocalAsyncEngine;
import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;
import socialite.dist.worker.WorkerNode;
import socialite.util.SociaLiteException;
import socialite.yarn.ClusterConf;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class DistEntry {
    private static final Log L = LogFactory.getLog(DistEntry.class);

    //-Dlog4j.configuration=file:/home/gengl/AsyncDatalog/conf/log4j.properties
    public static void main(String[] args) throws InterruptedException, NoSuchFieldException, IllegalAccessException, MPIException, IOException {
        if (System.getenv("OMPI_UNIVERSE_SIZE") == null) {
            throw new RuntimeException("Run me by mpirun");
        }
        MPI.Init(args);
        int machineNum = MPI.COMM_WORLD.getSize();
        int machineId = MPI.COMM_WORLD.getRank();
        int workerNum = machineNum - 1;
        L.info("Machine " + machineId + " Xmx " + Runtime.getRuntime().maxMemory() / 1024 / 1024);
        if (machineNum - 1 != ClusterConf.get().getNumWorkers())
            throw new SociaLiteException(String.format("MPI Workers (%d)!= Socialite Workers (%d)", workerNum, ClusterConf.get().getNumWorkers()));
        if (machineId == 0) {
            AsyncConfig.parse(TextUtils.readText(args[0]));
            L.info("master started");
            MasterNode.startMasterNode();
            AsyncMaster asyncMaster = new AsyncMaster(AsyncConfig.get().getDatalogProg());
            asyncMaster.startMaster();
//                IntStream.rangeClosed(1, workerNum).parallel().forEach(dest ->
//                        MPI.COMM_WORLD.send(new byte[1], 1, MPI.BYTE, dest, MsgType.EXIT.ordinal()));
        } else {
            L.info("Worker Started " + machineId);
            WorkerNode.startWorkerNode();
            AsyncWorker worker = new AsyncWorker();
            worker.startWorker();
//                MPI.COMM_WORLD.Recv(new byte[1], 0, 1, MPI.BYTE, 0, MsgType.EXIT.ordinal());
        }
        MPI.Finalize();
        L.info("process " + machineId + " exit.");
    }

}
