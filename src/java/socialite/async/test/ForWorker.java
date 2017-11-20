package socialite.async.test;

import gnu.trove.map.TIntFloatMap;
import socialite.async.dist.worker.AsyncWorker;
import socialite.dist.worker.WorkerNode;
import socialite.parser.Table;
import socialite.resource.DistTablePartitionMap;
import socialite.resource.SRuntimeWorker;
import socialite.resource.TableInstRegistry;

import java.util.Map;

public class ForWorker {
    public static void main(String[] args) throws InterruptedException {
        WorkerNode.startWorkerNode();

//        new Thread(() -> {
//            AsyncWorker worker = null;
//            try {
//                worker = new AsyncWorker();
//                worker.startWorker();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }).start();
    }

    public static Runnable runnable = () -> {
        SRuntimeWorker runtimeWorker = SRuntimeWorker.getInst();
        TIntFloatMap map = runtimeWorker.getProgress();
        Map<String, Table> tableMap =runtimeWorker.getTableMap();
        DistTablePartitionMap partitionMap = runtimeWorker.getPartitionMap();
        TableInstRegistry tableInstRegistry = runtimeWorker.getTableRegistry();
    };
}
