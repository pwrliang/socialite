package socialite.async.test;

import socialite.async.AsyncConfig;
import socialite.async.dist.master.AsyncMaster;
import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;
import socialite.engine.ClientEngine;

public class ForMaster {
    public static void main(String[] args) throws InterruptedException {

        //-Dsocialite.master=gengl -Dsocialite.worker.num=1 -Dlog4j.configuration=file:/home/gongsf/socialite/conf/log4j.properties -Dsocialite.output.dir=gen
        //~/socialite/examples/prog1.dl
        AsyncConfig.parse(TextUtils.readText(args[0]));
        MasterNode.startMasterNode();
        while (!MasterNode.getInstance().allOneLine())
            Thread.sleep(100);
        ClientEngine clientEngine = new ClientEngine();
        clientEngine.run("edge(int src:0..4,(int dst,int weight)).");
        clientEngine.run("edge(s,t,w) :- l=$read(\"/home/gongsf/socialite/examples/prog1_edge.txt\"),(s1, s2, s3)=$split(l, \"\t\"),s=$toInt(s1),t=$toInt(s2),w=$toInt(s3).");
        clientEngine.test();
//        AsyncMaster asyncMaster = new AsyncMaster(AsyncConfig.get().getDatalogProg());
//        asyncMaster.startMaster();
    }
}
