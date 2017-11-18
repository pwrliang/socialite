package socialite.async.test;

import socialite.async.AsyncConfig;
import socialite.async.dist.master.AsyncMaster;
import socialite.async.util.TextUtils;
import socialite.dist.master.MasterNode;

public class ForMaster {
    public static void main(String[] args) throws InterruptedException {

        //-Dsocialite.master=gengl -Dsocialite.worker.num=1 -Dlog4j.configuration=file:/home/gongsf/socialite/conf/log4j.properties -Dsocialite.output.dir=gen
        //~/socialite/examples/prog1.dl
        AsyncConfig.parse(TextUtils.readText(args[0]));
        MasterNode.startMasterNode();
        AsyncMaster asyncMaster = new AsyncMaster(AsyncConfig.get().getDatalogProg());
        asyncMaster.startMaster();
    }
}
