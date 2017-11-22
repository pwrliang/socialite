package socialite.async.test;

import socialite.dist.master.MasterNode;
import socialite.engine.ClientEngine;
import socialite.engine.LocalEngine;
import socialite.tables.QueryVisitor;

public class LoadTest {
    public static void main(String[] args) {
//        MasterNode.startMasterNode();
//        while (!MasterNode.getInstance().allOneLine()) ;
//        ClientEngine clientEngine = new ClientEngine();
//        clientEngine.run("Edge(int s, int t).");
//        clientEngine.run("Edge(s, t) :- l=$read(\"/home/gongsf/socialite/examples/prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"),\n" +
//                "             s=$toInt(s1), t=$toInt(s2).");
//        clientEngine.shutdown();
        LocalEngine localEngine =new LocalEngine();
        localEngine.run("Edge(int x, (int y)).");
        localEngine.run("Edge(s, t) :- l=$read(\"/home/gengl/socialite/examples/prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"),\n" +
                "             s=$toInt(s1), t=$toInt(s2).");
    }
}
