package socialite.async.test;

import socialite.async.dist.worker.AsyncWorker;
import socialite.dist.worker.WorkerNode;

public class ForWorker {
    public static void main(String[] args) throws InterruptedException {
        WorkerNode.startWorkerNode();
        AsyncWorker worker = new AsyncWorker();
        worker.startWorker();
    }
}
