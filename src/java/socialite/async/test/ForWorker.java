package socialite.async.test;

import socialite.async.dist.worker.AsyncWorker;
import socialite.dist.worker.WorkerNode;
import socialite.resource.DistTablePartitionMap;

public class ForWorker {
    public static void main(String[] args) throws InterruptedException {
        new Thread(() -> {
            AsyncWorker worker = null;
            try {
                worker = new AsyncWorker();
                worker.startWorker();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
        WorkerNode.startWorkerNode();


    }
}
