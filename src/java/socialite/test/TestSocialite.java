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
        String pagerank = "Node(int n:0..4).\n" +
                "Rank(int n:0..4, double rank).\n" +
                "Edge(int n:0..4, (int t)).\n" +
                "EdgeCnt(int n:0..4, int cnt).\n" +
                "Edge(s, t) :- l=$read(\"C:\\\\Users\\\\acer\\\\IdeaProjects\\\\socialite\\\\examples\\\\prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"), s=$toInt(s1), t=$toInt(s2).\n" +
                "EdgeCnt(s, $inc(1)) :- Edge(s, t).\n" +
                "Node(n) :- l=$read(\"C:\\\\Users\\\\acer\\\\IdeaProjects\\\\socialite\\\\examples\\\\prog2_node.txt\"), n=$toInt(l).\n" +
                "Rank(n, r) :- Node(n), r = 0.2 / 4.\n";
        localEngine.run(pagerank);
        localEngine.run("?- Rank(n, r).", new QueryVisitor() {
            @Override
            public boolean visit(Tuple _0) {
                return super.visit(_0);
            }
        });
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
        String pagerank = "Node(int n:0..4) shardby n.\n" +
                "Rank(int n:0..4, double rank) shardby n.\n" +
                "Edge(int n:0..4, (int t)) shardby n.\n" +
                "EdgeCnt(int n:0..4, int cnt) shardby n.\n" +
                "Edge(s, t) :- l=$read(\"C:\\\\Users\\\\acer\\\\IdeaProjects\\\\socialite\\\\examples\\\\prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"), s=$toInt(s1), t=$toInt(s2).\n" +
                "EdgeCnt(s, $inc(1)) :- Edge(s, t).\n" +
                "Node(n) :- l=$read(\"C:\\\\Users\\\\acer\\\\IdeaProjects\\\\socialite\\\\examples\\\\prog2_node.txt\"), n=$toInt(l).\n" +
                "Rank(n, r) :- Node(n), r = 0.2 / 4.\n";
        clientEngine.run(pagerank);
        clientEngine.run("?- Rank(n, r).", new QueryVisitor() {
            @Override
            public boolean visit(Tuple _0) {
                return super.visit(_0);
            }
        },0);
        L.info(status.getMemStatus());
    }
}
