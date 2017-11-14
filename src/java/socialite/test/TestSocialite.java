package socialite.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.dist.Status;
import socialite.dist.master.MasterNode;
import socialite.dist.worker.WorkerNode;
import socialite.engine.ClientEngine;
import socialite.engine.LocalEngine;
import socialite.tables.QueryVisitor;
import socialite.tables.Tuple;

public class TestSocialite {
    public static final Log L = LogFactory.getLog(TestSocialite.class);

    //master client
    //-Dsocialite.worker.num=1 -Dlog4j.configuration=file:C:\Users\acer\IdeaProjects\socialite\conf\log4j.properties -Dsocialite.output.dir=gen
    //worker
    //-Xmx1500M -Dlog4j.configuration=file:C:\Users\acer\IdeaProjects\socialite\conf\log4j.properties
    public static void main(String[] args) throws InterruptedException {
//        localTest();
        distTest();
    }

    static void localTest() {
        LocalEngine localEngine = new LocalEngine();
//        String pagerank = "Node(int n:0..4).\n" +
//                "Rank(int n:0..4, double rank).\n" +
//                "Rank1(int n:0..4, double rank).\n" +
//                "Edge(int n:0..4, (int t)).\n" +
//                "EdgeCnt(int n:0..4, int cnt).\n" +
//                "Edge(s, t) :- l=$read(\"C:\\\\Users\\\\acer\\\\IdeaProjects\\\\socialite\\\\examples\\\\prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"), s=$toInt(s1), t=$toInt(s2).\n" +
//                "EdgeCnt(s, $inc(1)) :- Edge(s, t).\n" +
//                "Node(n) :- l=$read(\"C:\\\\Users\\\\acer\\\\IdeaProjects\\\\socialite\\\\examples\\\\prog2_node.txt\"), n=$toInt(l).\n" +
//                "Rank(n, r) :- Node(n), r = 0.2 / 4.\n" +
//                "Rank1(y, $sum(r1)) :- Rank(x, r), Edge(x, y),  EdgeCnt(x, d), r1 = 0.8 * r / d.";
//        localEngine.run(pagerank);
//        localEngine.run("?- Rank1(n, r).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                return super.visit(_0);
//            }
//        });
        localEngine.run("Edge(int n:0..4, (int t)).\n"  +
                "Edge(s, t) :- l=$read(\"C:\\\\Users\\\\acer\\\\IdeaProjects\\\\socialite\\\\examples\\\\prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"), s=$toInt(s1), t=$toInt(s2).\n" );

        localEngine.shutdown();
    }

    static void distTest() throws InterruptedException {
        MasterNode.startMasterNode();
        L.info("OK");
        while (!MasterNode.getInstance().allOneLine())
            Thread.sleep(100);
        ClientEngine clientEngine = new ClientEngine();
        clientEngine.info();
        Status status = clientEngine.status();
//        clientEngine.run("Edge(int x:0..5, int y).");
//        clientEngine.run("Edge1(int x, int y).");
//        String pagerank = "Node(int n:0..3).\n" +
//                "Rank(int n:0..3, double rank).\n" +
//                "Rank1(int n:0..3, double rank).\n" +
//                "Edge(int n:0..3, (int t)).\n" +
//                "EdgeCnt(int n:0..3, int cnt).\n" +
//                "Edge(s, t) :- l=$read(\"E:\\\\Liang_Projects\\\\socialite\\\\examples\\\\prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"), s=$toInt(s1), t=$toInt(s2).\n" +
//                "EdgeCnt(s, $inc(1)) :- Edge(s, t).\n" +
//                "Node(n) :- l=$read(\"E:\\\\Liang_Projects\\\\socialite\\\\examples\\\\prog2_node.txt\"), n=$toInt(l).\n" +
//                "Rank(n, r) :- Node(n), r = 0.2 / 4.\n" +
//                "Rank1(y, $sum(r1)) :- Rank(x, r), Edge(x, y),  EdgeCnt(x, d), r1 = 0.8 * r / d.";
//        clientEngine.run(pagerank);
//        clientEngine.run("?- Rank1(n, r).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                return super.visit(_0);
//            }
//        }, 0);

//        clientEngine.run("Edge(int n:0..3, (int t)).\n" +
//                "Edge(s, t) :- l=$read(\"E:\\\\Liang_Projects\\\\socialite\\\\prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"), s=$toInt(s1), t=$toInt(s2).\n");
//        clientEngine.run("?- Edge(n, t).", new QueryVisitor() {
//            @Override
//            public boolean visit(Tuple _0) {
//                return super.visit(_0);
//            }
//        }, 0);

        clientEngine.run("Node(int n). \nNode(n) :- l=$read(\"E:\\\\Liang_Projects\\\\socialite\\\\examples\\\\prog2_node.txt\"), n=$toInt(l).\n");
        clientEngine.run("?- Node(n).", new QueryVisitor() {
            @Override
            public boolean visit(int _0) {
                return super.visit(_0);
            }
        },0);
        L.info(status.getMemStatus());
        //defaultPartitionNum=threadnum * 8 -> powerOf2
        //totalPartitionNum = defaultPartitionNum * maxNumWorkers
        //maxNumWorkers = workers * 4 -> powerOf2
//        0	0
//        1	1
//        2	2
//        3	2
//        4	3
//        5	3
//        6	3
//        7	3
//        8	4
//        9	4
//        10	4
//        11	4
//        12	4
//        13	4
//        14	4
//        15	4
//        16	5
    }
}
