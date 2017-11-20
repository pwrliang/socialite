package socialite.async.test;

import socialite.dist.master.MasterNode;
import socialite.engine.ClientEngine;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;

public class ForMaster {


    public static void main(String[] args) throws InterruptedException {
//        LocalEngine localEngine = new LocalEngine();
//        localEngine.run("edge1(int src:0..875712, (int dst)).\n" +
//                "edge2(int src:0..875712, (int dst)).\n" +
//                "edge_join(int src:0..875712, (int dst)).\n" +
//                "edge1(s,t) :- l=$read(\"hdfs://master:9000/Datasets/PageRank/Google/edge.txt\"),(s1, s2)=$split(l, \"\t\"),s=$toInt(s1),t=$toInt(s2).\n" +
//                "edge2(s,t) :- edge1(s, t).\n" +
//                "edge_join(src, dst) :- edge1(src, d), edge2(d, dst).");
//        {
//            int[] tmp = new int[1];
//            long[] sum = new long[1];
//            localEngine.run("?- edge_join(s, t).", new QueryVisitor() {
//                @Override
//                public boolean visit(Tuple _0) {
//                    sum[0] += _0.getInt(0);
//                    synchronized (tmp) {
//                        tmp[0]++;
//                    }
//                    return true;
//                }
//            });
//            System.out.println(tmp[0]);
//        }
//        localEngine.shutdown();
//        System.exit(0);
        //-Dsocialite.master=gengl -Dsocialite.worker.num=1 -Dlog4j.configuration=file:/home/gongsf/socialite/conf/log4j.properties -Dsocialite.output.dir=gen
        //~/socialite/examples/prog1.dl
        MasterNode.startMasterNode();
        while (!MasterNode.getInstance().allOneLine())
            Thread.sleep(100);
        ClientEngine clientEngine = new ClientEngine();
        clientEngine.run("edge(int src, int dst).");
        clientEngine.run("edge1(int src:0..875712, (int dst)).");
//        clientEngine.run("edge(s,t) :- l=$read(\"hdfs://master:9000/Datasets/PageRank/wikipedia_link_en/edge_pair.txt\"),(s1, s2)=$split(l, \"\t\"),s=$toInt(s1),t=$toInt(s2).");
        clientEngine.run("edge(s,t) :- l=$read(\"hdfs://master:9000/Datasets/PageRank/Google/edge.txt\"),(s1, s2)=$split(l, \"\t\"),s=$toInt(s1),t=$toInt(s2).\n" +
                "edge1(src,dst):-edge(src,dst).");

//        clientEngine.run("edge1(int src:0..875712, (int dst)).\n" +
//                "edge2(int src:0..875712, (int dst)).\n" +
//                "edge_join(int src:0..875712, (int dst)).\n" +
//                "edge1(s,t) :- l=$read(\"hdfs://master:9000/Datasets/PageRank/Google/edge.txt\"),(s1, s2)=$split(l, \"\t\"),s=$toInt(s1),t=$toInt(s2).\n" +
//                "edge2(s,t) :- edge1(s, t).\n" +
//                "edge_join(src, dst) :- edge1(src, d), edge2(d, dst).");
//        {
        int[] tmp = new int[1];
        long[] sum = new long[1];
        clientEngine.run("?- edge(s, t).", new QueryVisitor() {
            @Override
            public boolean visit(Tuple _0) {
                sum[0] += _0.getInt(0);
                synchronized (tmp) {
                    tmp[0]++;
                }
                return true;
            }
        }, 0);
        System.out.println(tmp[0]);
//        }
//        clientEngine.shutdown();

//        clientEngine.test();
//        AsyncMaster asyncMaster = new AsyncMaster(AsyncConfig.get().getDatalogProg());
//        asyncMaster.startMaster();
    }
}
