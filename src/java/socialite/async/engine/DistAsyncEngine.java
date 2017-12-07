package socialite.async.engine;

import org.apache.commons.lang.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.codegen.AsyncCodeGenMain;
import socialite.async.codegen.BaseAsyncRuntime;
import socialite.async.dist.MsgType;
import socialite.async.dist.Payload;
import socialite.async.util.NetworkThread;
import socialite.async.util.SerializeTool;
import socialite.codegen.Analysis;
import socialite.engine.ClientEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;
import socialite.util.SociaLiteException;
import socialite.yarn.ClusterConf;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DistAsyncEngine implements Runnable {
    private static final Log L = LogFactory.getLog(DistAsyncEngine.class);
    private int workerNum;
    private ClientEngine clientEngine;
    private AsyncAnalysis asyncAnalysis;
    private AsyncCodeGenMain asyncCodeGenMain;
    private NetworkThread networkThread;
    private StringBuilder datalogStats;

    public DistAsyncEngine(String program) {
        Parser parser = new Parser(program);
        parser.parse(program);
        Analysis tmpAn = new Analysis(parser);

        asyncAnalysis = new AsyncAnalysis(tmpAn);
        datalogStats = new StringBuilder();
        clientEngine = new ClientEngine();
        workerNum = ClusterConf.get().getNumWorkers();
        networkThread = NetworkThread.get();
        L.info("PROG:" + program);
        tmpAn.run();


        List<Rule> rules = tmpAn.getRules().stream().filter(rule -> !(rule instanceof DeltaRule)).collect(Collectors.toList());
        List<String> decls = parser.getTableDeclMap().values().stream().map(TableDecl::getDeclText).collect(Collectors.toList());

        //由socialite执行表创建和非递归规则
        if (!AsyncConfig.get().isDebugging())
            decls.forEach(decl -> datalogStats.append(decl).append("\n"));

        if (rules.stream().noneMatch(Rule::inScc))
            throw new SociaLiteException("This Datalog program has no recursive statements");
        //create tables
        if (!AsyncConfig.get().isDebugging())
            decls.forEach(decl -> datalogStats.append(decl).append("\n"));
        for (Rule rule : rules) {
            boolean added = false;
            if (rule.inScc() && rule.getDependingRules().size() > 0) {
                asyncAnalysis.addRecRule(rule);
                added = true;
            }
            if (!AsyncConfig.get().isDebugging())
                if (!added) {
                    datalogStats.append(rule.getRuleText()).append("\n");
                }
        }
    }

    private void compile() {
        if (asyncAnalysis.analysis()) {
            asyncCodeGenMain = new AsyncCodeGenMain(asyncAnalysis);
            asyncCodeGenMain.generateDist();
        }
    }

    @Override
    public void run() {
        compile();
        runReally();
        sendCmd();
        FeedBackThread feedBackThread = new FeedBackThread();
        feedBackThread.start();
        try {
            feedBackThread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        clientEngine.shutdown();
    }

    private void runReally() {
        networkThread.start();
        clientEngine.run(datalogStats.toString());
    }

    private void sendCmd() {
        SerializeTool serializeTool = new SerializeTool.Builder().build();


        LinkedHashMap<String, byte[]> compiledClasses = asyncCodeGenMain.getCompiledClasses();
        Payload payload = new Payload(AsyncConfig.get(), compiledClasses);
        payload.setRecTableName(asyncAnalysis.getRecPName());
        payload.setEdgeTableName(asyncAnalysis.getEdgePName());
        payload.setExtraTableName(asyncAnalysis.getExtraPName());
        byte[] data = serializeTool.toBytes(payload);
        L.info("worker num:" + workerNum);
        IntStream.rangeClosed(1, workerNum).forEach(dest ->
                networkThread.send(data, dest, MsgType.NOTIFY_INIT.ordinal())
        );
    }

    private class FeedBackThread extends Thread {
        SerializeTool serializeTool;
        private AsyncConfig asyncConfig;
        private StopWatch stopWatch;
        private Double lastSum;
        double accumulatedSum = 0;
        double totalUpdateTimes = 0;
        private int iter;

        private FeedBackThread() {
            asyncConfig = AsyncConfig.get();
            serializeTool = new SerializeTool.Builder().build();
        }

        @Override
        public void run() {
            double[] partialValue = new double[4];
            boolean[] termOrNot = new boolean[1];
            if (stopWatch == null) {
                stopWatch = new StopWatch();
                stopWatch.start();
            }
            try {
                while (true) {
                    accumulatedSum = 0;
                    double totalRx = 0, totalTx = 0;
                    if (asyncConfig.getEngineType() != AsyncConfig.EngineType.ASYNC) {
                        for (int source = 1; source <= workerNum; source++) {
                            byte[] data = networkThread.read(source, MsgType.REQUIRE_TERM_CHECK.ordinal());
                            partialValue = serializeTool.fromBytes(data, partialValue.getClass());
                            accumulatedSum += partialValue[0];
                            totalUpdateTimes += partialValue[1];
                            totalRx += partialValue[2];
                            totalTx += partialValue[3];
                        }
//                        //when first received feedback, we start stopwatch
                        termOrNot[0] = isTerm();
                        if (asyncConfig.isNetworkInfo())
                            L.info(String.format("RX: %f MB TX: %f MB", totalRx / 1024 / 1024, totalTx / 1024 / 1024));
                        byte[] data = serializeTool.toBytes(termOrNot);
                        IntStream.rangeClosed(1, workerNum).forEach(dest ->
                                networkThread.send(data, dest, MsgType.TERM_CHECK_FEEDBACK.ordinal()));
                        if (termOrNot[0]) {
                            stopWatch.stop();
                            L.info("SYNC/BARRIER MODE - TERM_CHECK_DETERMINED_TO_STOP ELAPSED " + stopWatch.getTime());
                            break;
                        }
                    } else {
                        //sleep first to prevent stop before compute
                        Thread.sleep(AsyncConfig.get().getCheckInterval());

                        //require all workers to partial aggregate delta/value
                        IntStream.rangeClosed(1, workerNum).forEach(dest -> networkThread.send(new byte[1], dest, MsgType.REQUIRE_TERM_CHECK.ordinal()));
                        //receive partial value/delta and network traffic from all workers
                        for (int source = 1; source <= workerNum; source++) {
                            byte[] data = networkThread.read(source, MsgType.TERM_CHECK_PARTIAL_VALUE.ordinal());

                            partialValue = serializeTool.fromBytes(data, partialValue.getClass());


                            accumulatedSum += partialValue[0];
                            totalUpdateTimes += partialValue[1];
                            totalRx += partialValue[2];
                            totalTx += partialValue[3];
                        }

                        termOrNot[0] = isTerm();
                        if (asyncConfig.isNetworkInfo())
                            L.info(String.format("RX: %f MB TX: %f MB", totalRx / 1024 / 1024, totalTx / 1024 / 1024));

                        IntStream.rangeClosed(1, workerNum).forEach(dest -> {
                            byte[] data = serializeTool.toBytes(termOrNot);
                            networkThread.send(data, dest, MsgType.TERM_CHECK_FEEDBACK.ordinal());
                        });
                        if (termOrNot[0]) {
                            stopWatch.stop();
                            L.info("ASYNC MODE - TERM_CHECK_DETERMINED_TO_STOP ELAPSED " + stopWatch.getTime());
                            break;
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private boolean isTerm() {
            L.info("TOTAL UPDATE TIMES " + totalUpdateTimes);
            if (asyncConfig.getEngineType() != AsyncConfig.EngineType.ASYNC) {
                L.info("ITER: " + ++iter);
            }
            if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.VALUE)
                L.info("TERM_CHECK_VALUE_SUM: " + new BigDecimal(accumulatedSum));
            else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DELTA)
                L.info("TERM_CHECK_DELTA_SUM: " + new BigDecimal(accumulatedSum));
            else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_VALUE) {
                if (lastSum == null) {
                    lastSum = accumulatedSum;
                    return false;
                } else {
                    double tmp = accumulatedSum;
                    accumulatedSum = Math.abs(lastSum - accumulatedSum);
                    lastSum = tmp;
                }
                L.info("TERM_CHECK_DIFF_VALUE_SUM: " + new BigDecimal(accumulatedSum));
            } else if (asyncConfig.getCheckType() == AsyncConfig.CheckerType.DIFF_DELTA) {
                if (lastSum == null) {
                    lastSum = accumulatedSum;
                    return false;
                } else {
                    double tmp = accumulatedSum;
                    accumulatedSum = Math.abs(lastSum - accumulatedSum);
                    lastSum = tmp;
                }
                L.info("TERM_CHECK_DIFF_DELTA_SUM: " + new BigDecimal(accumulatedSum));
            }
            return BaseAsyncRuntime.eval(accumulatedSum);
        }
    }
}
