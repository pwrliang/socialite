package socialite.async.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.dist.worker.WorkerNode;
import socialite.parser.Table;
import socialite.resource.DistTablePartitionMap;
import socialite.resource.SRuntimeWorker;
import socialite.resource.TableInstRegistry;
import socialite.tables.TableInst;
import socialite.visitors.VisitorImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ForWorker {
    public static final Log L = LogFactory.getLog(ForWorker.class);

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
        System.out.println("call test");
        SRuntimeWorker runtimeWorker = SRuntimeWorker.getInst();
        Map<String, Table> tableMap = runtimeWorker.getTableMap();
        DistTablePartitionMap partitionMap = runtimeWorker.getPartitionMap();
        TableInstRegistry tableInstRegistry = runtimeWorker.getTableRegistry();
        Table edge = tableMap.get("edge");
        TableInst[] tableInsts = tableInstRegistry.getTableInstArray(edge.id());
        for (TableInst tableInst : tableInsts) {
            if (!tableInst.isEmpty()) {
                Class<TableInst> tableInstClass = (Class<TableInst>) tableInst.getClass();
                try {
                    Method method = tableInstClass.getMethod("iterate", VisitorImpl.class);
                    try {
                        method.invoke(tableInst, new VisitorImpl() {
                            boolean called;
                            @Override
                            public boolean visit_0(int a1) {
                                if(!partitionMap.isLocal(edge.id(),a1)){
                                    L.error("error partition");
                                }
//                                if(!called) {
//                                    L.info(String.format("Worker %d first %d", runtimeWorker.getWorkerAddrMap().myIndex(), a1));
//                                    called = true;
//                                }
                                return false;
                            }

                            @Override
                            public boolean visit(int a1) {
                                return false;
                            }
                        });

                    } catch (IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
            }
        }

    };
}
