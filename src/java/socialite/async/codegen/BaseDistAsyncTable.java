package socialite.async.codegen;


import org.apache.commons.lang3.time.StopWatch;
import socialite.async.AsyncConfig;
import socialite.async.util.SerializeTool;
import socialite.resource.DistTablePartitionMap;
import socialite.visitors.VisitorImpl;
import socialite.yarn.ClusterConf;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerArray;

public abstract class BaseDistAsyncTable extends BaseAsyncTable {
    private final AtomicIntegerArray messageTableSelector;
    private final MessageTableBase[][] messageTableList;
    private final MessageTableBase[] messageTableList1;

    protected final int workerNum;
    protected final int myWorkerId;
    protected final DistTablePartitionMap partitionMap;
    protected final int tableIdForIndex;

    protected final int messageTableUpdateThreshold;
    protected final int initSize;

    public BaseDistAsyncTable(Class<?> messageTableClass, DistTablePartitionMap partitionMap, int tableIdForIndex) {
        this.workerNum = ClusterConf.get().getNumWorkers();
        this.myWorkerId = ClusterConf.get().getRank() - 1;
        this.partitionMap = partitionMap;
        this.tableIdForIndex = tableIdForIndex;
        this.messageTableUpdateThreshold = AsyncConfig.get().getMessageTableUpdateThreshold();
        this.initSize = AsyncConfig.get().getInitSize();


        messageTableSelector = new AtomicIntegerArray(workerNum);
        messageTableList = new MessageTableBase[workerNum][2];
        try {
            Constructor constructor = messageTableClass.getConstructor();

            for (int wid = 0; wid < workerNum; wid++) {
                if (wid == myWorkerId) continue;//for worker i, it have 0,1,...,i-1,null,i+1,...n-1 buffer table
                messageTableList[wid][0] = (MessageTableBase) constructor.newInstance();
                messageTableList[wid][1] = (MessageTableBase) constructor.newInstance();
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
        }
        messageTableList1 = new MessageTableBase[workerNum];
        try {
            Constructor constructor = messageTableClass.getConstructor();

            for (int wid = 0; wid < workerNum; wid++) {
                if (wid == myWorkerId) continue;//for worker i, it have 0,1,...,i-1,null,i+1,...n-1 buffer table
                messageTableList1[wid] = (MessageTableBase) constructor.newInstance();
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
        }
    }

//    public AtomicIntegerArray getMessageTableSelector() {
//        return messageTableSelector;
//    }

    public MessageTableBase[][] getMessageTableList() {
        return messageTableList;
    }

    public MessageTableBase getWritableMessageTable(int workerId) {
//        return messageTableList[workerId][messageTableSelector.get(workerId)];
        return messageTableList1[workerId];
    }

    StopWatch stopWatch = new StopWatch();
    long serialTime = 0;

    public byte[] getSendableMessageTableBytes(int sendToWorkerId, SerializeTool serializeTool) throws InterruptedException {
        int writingTableInd;
        writingTableInd = messageTableSelector.get(sendToWorkerId);//获取计算线程正在写入的表序号
        MessageTableBase sendableMessageTable = messageTableList[sendToWorkerId][writingTableInd];
        long startTime = System.currentTimeMillis();
        //in sync mode, all computing thread write to message table when barrier is triggered, so we don't have to wait
        while (AsyncConfig.get().getEngineType() == AsyncConfig.EngineType.ASYNC &&
                sendableMessageTable.size() < messageTableUpdateThreshold) {
//            Thread.sleep(1);
            if ((System.currentTimeMillis() - startTime) >= AsyncConfig.get().getMessageTableWaitingInterval())
                break;
        }
        swtichTimes.addAndGet(1);
        messageTableSelector.set(sendToWorkerId, writingTableInd == 0 ? 1 : 0);
        // sleep to ensure switched, this is important
        // even though selector is atomic type, but computing thread cannot see the switched result immediately, i don't know why :(
        Thread.sleep(10);
        stopWatch.reset();
        stopWatch.start();
        byte[] data = serializeTool.toBytes(sendableMessageTable.size() * (1000), sendableMessageTable);
        stopWatch.stop();
        System.out.println("average:" + (data.length / sendableMessageTable.size()));
        System.out.println("serial time " + stopWatch.getTime());
        sendableMessageTable.resetDelta();
        return data;
    }

    public ByteBuffer getSendableMessageTableByteBuffer(int sendToWorkerId, SerializeTool serializeTool) throws InterruptedException {
        int writingTableInd;
        writingTableInd = messageTableSelector.get(sendToWorkerId);//获取计算线程正在写入的表序号
//        MessageTableBase sendableMessageTable = messageTableList[sendToWorkerId][writingTableInd];
        MessageTableBase sendableMessageTable = messageTableList1[sendToWorkerId];
        long startTime = System.currentTimeMillis();
        //in sync mode, all computing thread write to message table when barrier is triggered, so we don't have to wait
        while (AsyncConfig.get().getEngineType() == AsyncConfig.EngineType.ASYNC &&
                sendableMessageTable.size() < messageTableUpdateThreshold
                ) {
//            Thread.sleep(1);
            if ((System.currentTimeMillis() - startTime) >= AsyncConfig.get().getMessageTableWaitingInterval())
                break;
        }
        swtichTimes.addAndGet(1);
        messageTableSelector.set(sendToWorkerId, writingTableInd == 0 ? 1 : 0);
        // sleep to ensure switched, this is important
        // even though selector is atomic type, but computing thread cannot see the switched result immediately, i don't know why :(
        stopWatch.reset();
        stopWatch.start();
//        System.out.println(sendableMessageTable.size());
        ByteBuffer buffer;
        synchronized (sendableMessageTable) {
            buffer = serializeTool.toByteBuffer(2048 + sendableMessageTable.size() * (8 + 8), sendableMessageTable);
            sendableMessageTable.resetDelta();
        }
        stopWatch.stop();
//        System.out.println("serial time " + stopWatch.getTime());
        return buffer;
    }

    public byte[] getSendableMessageTableByteBuffer1(int sendToWorkerId, SerializeTool serializeTool) throws InterruptedException {
        int writingTableInd;
        MessageTableBase sendableMessageTable = messageTableList1[sendToWorkerId];
        long startTime = System.currentTimeMillis();
        //in sync mode, all computing thread write to message table when barrier is triggered, so we don't have to wait
        while (AsyncConfig.get().getEngineType() == AsyncConfig.EngineType.ASYNC &&
                sendableMessageTable.size() < messageTableUpdateThreshold
                ) {
//            Thread.sleep(1);
            if ((System.currentTimeMillis() - startTime) >= AsyncConfig.get().getMessageTableWaitingInterval())
                break;
        }
        swtichTimes.addAndGet(1);
        // sleep to ensure switched, this is important
        // even though selector is atomic type, but computing thread cannot see the switched result immediately, i don't know why :(
        Thread.sleep(10);
        stopWatch.reset();
        stopWatch.start();
        //System.out.println(sendableMessageTable.size());
//        ByteBuffer buffer = serializeTool.toByteBuffer(2048+sendableMessageTable.size() * (100), sendableMessageTable);
        byte[] data;
        synchronized (sendableMessageTable) {
            data = serializeTool.toBytes(sendableMessageTable.size() * (1000), sendableMessageTable);
            stopWatch.stop();
            //System.out.println("serial time " + stopWatch.getTime());

            sendableMessageTable.resetDelta();
        }
        return data;
    }

    public abstract void applyBuffer(MessageTableBase messageTable);

    public VisitorImpl getEdgeVisitor() {
        throw new NotImplementedException();
    }

    public VisitorImpl getInitVisitor() {
        throw new NotImplementedException();
    }
}
