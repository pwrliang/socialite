package socialite.async.engine;

import socialite.async.AsyncConfig;
import socialite.async.analysis.AsyncAnalysis;
import socialite.async.codegen.AsyncCodeGenMain;
import socialite.async.codegen.AsyncRuntime;
import socialite.async.codegen.BaseAsyncTable;
import socialite.async.util.TextUtils;
import socialite.codegen.Analysis;
import socialite.engine.LocalEngine;
import socialite.parser.DeltaRule;
import socialite.parser.Parser;
import socialite.parser.Rule;
import socialite.parser.antlr.TableDecl;
import socialite.resource.TableInstRegistry;
import socialite.tables.QueryVisitor;
import socialite.tables.TableInst;
import socialite.tables.Tuple;
import socialite.util.SociaLiteException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;

public class LocalAsyncEngine {
    private AsyncAnalysis asyncAnalysis;
    private AsyncCodeGenMain asyncCodeGenMain;
    private LocalEngine localEngine;
    private StringBuilder datalogStats;

    public LocalAsyncEngine(String program) {
        Parser parser = new Parser(program);
        parser.parse(program);
        Analysis tmpAn = new Analysis(parser);

        //由socialite执行表创建和非递归规则
        asyncAnalysis = new AsyncAnalysis(tmpAn);
        datalogStats = new StringBuilder();
        localEngine = new LocalEngine();

        tmpAn.run();
        List<Rule> rules = tmpAn.getRules().stream().filter(rule -> !(rule instanceof DeltaRule)).collect(Collectors.toList());
        List<String> decls = parser.getTableDeclMap().values().stream().map(TableDecl::getDeclText).collect(Collectors.toList());

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
        //run non-recursive rules
    }

    private void compile() {
        if (asyncAnalysis.analysis()) {
            asyncCodeGenMain = new AsyncCodeGenMain(asyncAnalysis);
            asyncCodeGenMain.generateSharedMem();
        }
    }

    private void runReally(QueryVisitor queryVisitor) {
        localEngine.run(datalogStats.toString());//execute non-recursive rules
        TableInstRegistry registry = localEngine.getRuntime().getTableRegistry();

        TableInst[] recInst = registry.getTableInstArray(localEngine.getRuntime().getTableMap().get(asyncAnalysis.getRecPName()).id());
        TableInst[] edgeInst = registry.getTableInstArray(localEngine.getRuntime().getTableMap().get(asyncAnalysis.getEdgePName()).id());
        TableInst[] extraInstArr = null;
        if (asyncAnalysis.getExtraPName() != null) {
            extraInstArr = registry.getTableInstArray(localEngine.getRuntime().getTableMap().get(asyncAnalysis.getExtraPName()).id());
        }

        Class<?> klass = asyncCodeGenMain.getAsyncTable();
        try {
            Constructor<?> constructor = klass.getConstructor(int.class);
            BaseAsyncTable asyncTable = (BaseAsyncTable) constructor.newInstance(AsyncConfig.get().getInitSize());
            AsyncRuntime asyncRuntime = new AsyncRuntime(asyncTable, recInst, edgeInst, extraInstArr);
            asyncRuntime.run();
            asyncTable.iterate(queryVisitor);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        AsyncConfig asyncConfig = AsyncConfig.get();
        compile();
        //List<String> initStats = asyncCodeGenMain.getInitStats();

        if (!asyncConfig.isDebugging()) {
            //initStats.forEach(initStat -> localEngine.run(initStat));
            String savePath = AsyncConfig.get().getSavePath();
            TextUtils textUtils = null;
            if (savePath.length() > 0) {
                textUtils = new TextUtils(savePath, "part-0");
            }

            TextUtils finalTextUtils = textUtils;
            runReally(new QueryVisitor() {
                @Override
                public boolean visit(Tuple _0) {
                    if (asyncConfig.isPrintResult())
                        System.out.println(_0.toString());
                    if (finalTextUtils != null)
                        finalTextUtils.writeLine(_0.toString());
                    return true;
                }

                @Override
                public void finish() {
                    if (finalTextUtils != null)
                        finalTextUtils.close();
                }
            });//save result
        }
        localEngine.shutdown();
    }


}
