package socialite.async.dist.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.codegen.BaseAsyncRuntime;
import socialite.async.codegen.BaseDistAsyncTable;
import socialite.async.codegen.MessageTableBase;
import socialite.async.dist.MsgType;
import socialite.async.dist.Payload;
import socialite.async.util.NetworkThread;
import socialite.async.util.NetworkUtil;
import socialite.async.util.SerializeTool;
import socialite.parser.Table;
import socialite.resource.DistTablePartitionMap;
import socialite.resource.SRuntimeWorker;
import socialite.resource.TableInstRegistry;
import socialite.tables.TableInst;
import socialite.util.Assert;
import socialite.util.Loader;
import socialite.util.SociaLiteException;
import socialite.visitors.VisitorImpl;
import socialite.yarn.ClusterConf;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CyclicBarrier;

public class DistAsyncRuntime extends BaseAsyncRuntime {
    private static final Log L = LogFactory.getLog(DistAsyncRuntime.class);
    private final int myWorkerId;
    private final int workerNum;
    private NetworkThread networkThread;
    private Payload payload;
    private volatile boolean stopTransmiting;

    DistAsyncRuntime() {
        workerNum = ClusterConf.get().getNumWorkers();
        myWorkerId = ClusterConf.get().getRank() - 1;
        networkThread = new NetworkThread();
        networkThread.start();
    }

    @Override
    public void run() {
        waitingCmd();//waiting for AsyncConfig
        SRuntimeWorker runtimeWorker = SRuntimeWorker.getInst();
        TableInstRegistry tableInstRegistry = runtimeWorker.getTableRegistry();
        Map<String, Table> tableMap = runtimeWorker.getTableMap();
        TableInst[] initTableInstArr = tableInstRegistry.getTableInstArray(tableMap.get("InitTable").id());
        TableInst[] edgeTableInstArr = tableInstRegistry.getTableInstArray(tableMap.get(payload.getEdgeTableName()).id());
        if (loadData(initTableInstArr, edgeTableInstArr)) {//this worker is idle, stop
            createThreads();
            startThreads();
        } else {//this worker is idle
            throw new SociaLiteException("Worker " + myWorkerId + " is idle, please reduce the number of workers");
        }
    }

    private void waitingCmd() {
        byte[] data = new byte[1024 * 1024];
        int[] myIdxWorkerId = new int[]{SRuntimeWorker.getInst().getWorkerAddrMap().myIndex(), myWorkerId};
        SerializeTool serializeTool = new SerializeTool.Builder().build();
        //send myIdx->myWorkerId (which equals Rank - 1)
        networkThread.send(serializeTool.toBytes(myIdxWorkerId), 0, MsgType.REPORT_MYIDX.ordinal());
        L.info("myIdx - worker id sent");
        //read Payload(edge name, byte codes...)
        data = networkThread.read(0, MsgType.NOTIFY_INIT.ordinal());
        payload = serializeTool.fromBytes(data, Payload.class);
        AsyncConfig.set(payload.getAsyncConfig());
        L.info("RECV CMD NOTIFY_INIT CONFIG:" + AsyncConfig.get());
    }

    @Override
    protected boolean loadData(TableInst[] initTableInstArr, TableInst[] edgeTableInstArr) {
        Loader.loadFromBytes(payload.getByteCodes());
        Class<?> messageTableClass = Loader.forName("socialite.async.codegen.MessageTable");
        Class<?> distAsyncTableClass = Loader.forName("socialite.async.codegen.DistAsyncTable");
        try {
            SRuntimeWorker runtimeWorker = SRuntimeWorker.getInst();
            DistTablePartitionMap partitionMap = runtimeWorker.getPartitionMap();
            Map<Integer, Integer> myIdxWorkerMap = payload.getMyIdxWorkerIdMap();
            int[] myIdxWorkerArr = new int[myIdxWorkerMap.size()];
            myIdxWorkerMap.forEach((myIdx, workerId) -> myIdxWorkerArr[myIdx] = workerId);
            //static, int type key
            int indexForTableId;
            if (AsyncConfig.get().isDynamic()) {
                Method method = edgeTableInstArr[0].getClass().getMethod("tableid");
                indexForTableId = (Integer) method.invoke(edgeTableInstArr[0]);
                //public DistAsyncTable(Class\<?> messageTableClass, DistTableSliceMap sliceMap, int indexForTableId) {
                Constructor constructor = distAsyncTableClass.getConstructor(messageTableClass.getClass(), DistTablePartitionMap.class, int.class, int[].class);

                asyncTable = (BaseDistAsyncTable) constructor.newInstance(messageTableClass, partitionMap, indexForTableId, myIdxWorkerArr);

                for (TableInst edgeInst : edgeTableInstArr) {
                    //动态算法需要edge做连接，如prog4、9!>
                    method = edgeInst.getClass().getDeclaredMethod("iterate", VisitorImpl.class);
                    for (TableInst tableInst : edgeTableInstArr) {
                        if (!tableInst.isEmpty()) {
                            method.invoke(tableInst, asyncTable.getEdgeVisitor());
                            tableInst.clear();
                        }
                    }
                }
            } else {
                TableInst initTableInst = initTableInstArr[0];
                if (initTableInst == null) {
                    L.warn("worker " + myWorkerId + " has no job");
                    return false;
                }
                Method method = initTableInst.getClass().getMethod("tableid");
                indexForTableId = (Integer) method.invoke(initTableInst);
                Field baseField = initTableInstArr[0].getClass().getDeclaredField("base");
                baseField.setAccessible(true);
                int base = baseField.getInt(Arrays.stream(initTableInstArr).filter(tableInst -> !tableInst.isEmpty()).findFirst().orElse(null));
                //public DistAsyncTable(Class\<?> messageTableClass, DistTableSliceMap sliceMap, int indexForTableId, int base) {
                Constructor constructor = distAsyncTableClass.getConstructor(messageTableClass.getClass(), DistTablePartitionMap.class, int.class, int[].class, int.class);
                asyncTable = (BaseDistAsyncTable) constructor.newInstance(messageTableClass, partitionMap, indexForTableId, myIdxWorkerArr, base);
            }



            for (TableInst tableInst : initTableInstArr) {
                Method method = tableInst.getClass().getDeclaredMethod("iterate", VisitorImpl.class);
                if (!tableInst.isEmpty()) {
                    method.invoke(tableInst, asyncTable.getInitVisitor());
                    tableInst.clear();
                }
            }

            for (TableInst tableInst : edgeTableInstArr) {
                if (!tableInst.isEmpty()) {
                    tableInst.clear();
                }
            }
            System.gc();
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        L.info("WorkerId " + myWorkerId + " Data Loaded size:" + asyncTable.getSize());
        return true;
    }

    @Override
    protected void createThreads() {
        super.createThreads();
        arrangeTask();
        checkerThread = new CheckThread();
        if (AsyncConfig.get().isSync() || AsyncConfig.get().isBarrier())
            barrier = new CyclicBarrier(asyncConfig.getThreadNum(), checkerThread);
    }

    private void startThreads() {
        if (AsyncConfig.get().isPriority() && !AsyncConfig.get().isPriorityLocal()) schedulerThread.start();
        Arrays.stream(computingThreads).filter(Objects::nonNull).forEach(Thread::start);
        networkThread.start();
        if (!AsyncConfig.get().isSync() && !AsyncConfig.get().isBarrier()) {

            L.info("network thread started");
            checkerThread.start();
            L.info("checker started");
        }

        L.info(String.format("Worker %d all threads started.", myWorkerId));
        try {
            for (ComputingThread computingThread : computingThreads) computingThread.join();
            L.info("Worker " + myWorkerId + " Computing Threads exited.");

            if (!AsyncConfig.get().isSync() && AsyncConfig.get().isBarrier()) {
                checkerThread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class Sender extends Thread {
        private SerializeTool serializeTool;

        private Sender() {
            serializeTool = new SerializeTool.Builder()
                    .setSerializeTransient(true) //!!!!!!!!!!AtomicDouble's value field is transient
                    .build();
        }

        @Override
        public void run() {
            try {
                while (!stopTransmiting) {

                    for (int sendToWorkerId = 0; sendToWorkerId < workerNum; sendToWorkerId++) {
                        if (sendToWorkerId == myWorkerId) continue;
                        byte[] data = ((BaseDistAsyncTable) asyncTable).getSendableMessageTableBytes(sendToWorkerId, serializeTool);
                        networkThread.send(data, sendToWorkerId + 1, MsgType.MESSAGE_TABLE.ordinal());
                    }

                    if (AsyncConfig.get().isSync() || AsyncConfig.get().isBarrier()) break;
                }
//                L.info("end send");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class Receiver extends Thread {
        private SerializeTool serializeTool;
        private Class<?> klass;

        private Receiver() {
            serializeTool = new SerializeTool.Builder()
                    .setSerializeTransient(true)
                    .build();
            klass = Loader.forName("socialite.async.codegen.MessageTable");
        }

        @Override
        public void run() {
            while (!stopTransmiting) {
                for (int recvFromWorkerId = 0; recvFromWorkerId < workerNum; recvFromWorkerId++) {
                    if (recvFromWorkerId == myWorkerId) continue;
                    byte[] data = networkThread.read(recvFromWorkerId, MsgType.MESSAGE_TABLE.ordinal());
                    MessageTableBase messageTable = (MessageTableBase) serializeTool.fromBytesToObject(data, klass);
                    ((BaseDistAsyncTable) asyncTable).applyBuffer(messageTable);
//                        L.info("msg size: " + messageTable.size());
                }

                if (AsyncConfig.get().isSync() || AsyncConfig.get().isBarrier()) break;
            }
//                L.info("end recv");
        }
    }

    private class CheckThread extends BaseAsyncRuntime.CheckThread {
        private AsyncConfig asyncConfig;
        private SerializeTool serializeTool;


        private CheckThread() {
            asyncConfig = AsyncConfig.get();
            serializeTool = new SerializeTool.Builder().build();
        }

        @Override
        public void run() {
            super.run();
            boolean[] feedback = new boolean[2];
            while (true) {
                if (asyncConfig.isDynamic())
                    arrangeTask();
                long[] rxTx = new long[]{0, 0};
                if (asyncConfig.isNetworkInfo())
                    rxTx = NetworkUtil.getNetwork();
                if (asyncConfig.isSync() || asyncConfig.isBarrier()) {//sync mode
//                    Arrays.stream(sendThreads).forEach(SendThreadSingle::start);
//                    Arrays.stream(receiveThreads).forEach(ReceiveThreadSingle::start);
//                    waitNetworkThread();

                    double partialSum = update();

//                    MPI.COMM_WORLD.sendRecv(new double[]{partialSum, updateCounter.get(), rxTx[0], rxTx[1]}, 0, 4, MPI.DOUBLE, AsyncMaster.ID, MsgType.REQUIRE_TERM_CHECK.ordinal(),
//                            feedback, 0, 1, MPI.BOOLEAN, AsyncMaster.ID, MsgType.TERM_CHECK_FEEDBACK.ordinal());
                    if (feedback[0]) {
                        done();
                    } else {
//                        createNetworkThreads();//cannot reuse dead thread, we need recreate
                    }
                    break;//exit function, run will be called next round
                } else {
//                    L.info("switch times: " + asyncTable.swtichTimes.get());
                    double partialSum = update();

                    networkThread.read(0, MsgType.REQUIRE_TERM_CHECK.ordinal());

                    double[] data = new double[]{partialSum, updateCounter.get(), rxTx[0], rxTx[1]};
                    networkThread.send(serializeTool.toBytes(data), 0, MsgType.TERM_CHECK_PARTIAL_VALUE.ordinal());
                    byte[] feedBackData = networkThread.read(0, MsgType.TERM_CHECK_FEEDBACK.ordinal());
                    feedback = serializeTool.fromBytes(feedBackData, feedback.getClass());

                    if (feedback[0]) {
                        L.info("waiting for flush");
                        stopTransmiting = true;
                        networkThread.shutdown();
                        L.info("flushed");
                        done();
                        break;
                    }
                }
            }
        }


        private double update() {
            double partialSum = 0;
            if (asyncTable != null) {//null indicate this worker is idle
                if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA || asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_DELTA) {
                    partialSum = asyncTable.accumulateDelta();
//                    System.out.println(partialSum);
                    BaseDistAsyncTable baseDistAsyncTable = (BaseDistAsyncTable) asyncTable;
                    for (int workerId = 0; workerId < workerNum; workerId++) {
                        MessageTableBase messageTable1 = baseDistAsyncTable.getMessageTableList()[workerId][0];
                        MessageTableBase messageTable2 = baseDistAsyncTable.getMessageTableList()[workerId][1];
                        if (messageTable1 == null || messageTable2 == null) continue;
                        partialSum += messageTable1.accumulate();
                        partialSum += messageTable2.accumulate();
                    }
//                    L.info("partialSum of delta: " + new BigDecimal(partialSum));
                } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE || asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_VALUE) {
                    partialSum = asyncTable.accumulateValue();
//                    L.info("sum of value: " + new BigDecimal(partialSum));
                }
                //accumulate rest message
            }
            return partialSum;
        }

    }


}