package socialite.async.test;

import socialite.dist.master.MasterNode;
import socialite.engine.ClientEngine;

public class LoadTest {
    public static void main(String[] args) {
        MasterNode.startMasterNode();
        while (!MasterNode.getInstance().allOneLine()) ;
        ClientEngine clientEngine = new ClientEngine();
        clientEngine.run("Edge(int s, int t).");
        clientEngine.run("Edge(s, t) :- l=$read(\"/home/gongsf/socialite/examples/prog2_edge.txt\"), (s1,s2)=$split(l, \"\t\"),\n" +
                "             s=$toInt(s1), t=$toInt(s2).");
        clientEngine.shutdown();
    }
}
