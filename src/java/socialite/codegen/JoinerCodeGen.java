package socialite.codegen;

import gnu.trove.list.array.TIntArrayList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringEscapeUtils;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;

import socialite.dist.master.MasterNode;
import socialite.parser.*;
import socialite.parser.antlr.ColumnGroup;
import socialite.resource.SRuntimeMaster;
import socialite.tables.TableUtil;
import socialite.util.Assert;
import socialite.functions.Choice;
import socialite.yarn.ClusterConf;

public class JoinerCodeGen {
    static String visitorPackage = "socialite.visitors";

    Rule rule;
    STGroup combinerGroup;
    STGroup tmplGroup;
    ST visitorTmpl;
    String visitorName;

    Epoch epoch;
    Map<String, Table> tableMap;
    Map<String, ST> visitMethodMap = new HashMap<>();
    Map<Predicate, ST> visitorMap = new HashMap<>();
    Predicate headP, iterStartP;
    Table headT;
    DeltaTable deltaHeadT;
    Set<Variable>[] resolvedVarsArray;

    public JoinerCodeGen(Epoch e, Rule r, Map<String, Table> _tableMap) {
        epoch = e;
        rule = r;
        tableMap = _tableMap;

        visitorName = visitorClassName(rule, tableMap);

        headP = rule.getHead();
        headT = tableMap.get(headP.name());
        combinerGroup = CodeGen.getCombinedIterateGroup();
        tmplGroup = CodeGen.getVisitorGroup();
        visitorTmpl = tmplGroup.getInstanceOf("class");

        if (rule.inScc()) {
            if (headT instanceof DeltaTable) {
                deltaHeadT = (DeltaTable) tableMap.get(headT.name());
            } else {
                deltaHeadT = (DeltaTable) tableMap.get(DeltaTable.name(headT));
            }
        }

        resolvedVarsArray = Analysis.getResolvedVars(rule);
    }
    ST getCombinedIterte(TableType t1, TableType t2) {
        if (t1.isArrayTable() && t2.isArrayTable()) {
            return combinerGroup.getInstanceOf("combinedIterate_arr");
        }
        throw new AssertionError("Unsupported table types:"+t1+", "+t2);
    }
    boolean isDistributed() {
        return MasterNode.getInstance() != null;
    }
    String getVisitMethodSig(Predicate p, String visitName, Class... types) {
        String sig = p.name()+"."+p.getPos()+"."+visitName+"#";
        for (Class type : types) {
            sig += "#" + type.getSimpleName();
        }
        return sig;
    }

    ST getVisitMethod(Predicate p, int startCol, int endCol, int numColumns) {
        String name = "visit" + CodeGen.getVisitColumns(startCol, endCol, numColumns);
        Table t = getTable(p);
        Class[] argTypes = CodeGen.getArgTypes(t, startCol, endCol);
        String sig = getVisitMethodSig(p, name, argTypes);
        ST method = visitMethodMap.get(sig);
        if (method == null) {
            method = getNewVisitMethodTmpl(name);
            method.add("ret", "return true");
            CodeGen.addArgTypes(method, p, startCol, endCol);
            visitMethodMap.put(sig, method);
            ST visitor = visitorMap.get(p);
            visitor.add("methods", method);
        }
        return method;
    }

    ST getNewVisitMethodTmpl(String name) {
        ST m = tmplGroup.getInstanceOf("visitMethod");
        m.add("name", name);
        return m;
    }
    ST getNewMethodTmpl(String name, String type) {
        return getNewMethodTmpl(name, "public", type);
    }

    ST getNewMethodTmpl(String name, String modifier, String type) {
        ST m = tmplGroup.getInstanceOf("method");
        m.add("modifier", modifier);
        m.add("type", type);
        m.add("name", name);
        return m;
    }

    public String visitorName() {
        return visitorPackage + "." + visitorName;
    }

    boolean isGenerated() {
        return visitorTmpl.getAttribute("name") != null;
    }
    public String generate() {
        if (isGenerated()) { return visitorTmpl.render(); }

        visitorTmpl.add("packageStmt", "package " + visitorPackage);
        visitorTmpl.add("modifier", "public");
        visitorTmpl.add("name", visitorName);

        visitorTmpl.add("extends", "extends Joiner");
        visitorTmpl.add("interfaces", "Runnable");

        importTablesDeclareFields();
        generateConstructor();
        generateMethods();
        return visitorTmpl.render();
    }

    private void importTablesDeclareFields() {
        for (Predicate p : rule.getAllP()) {
            String var = getVarName(p);
            Table t = getTable(p);
            String tableClass = t.className();

            visitorTmpl.add("imports", TableUtil.getTablePath(tableClass));
            if (isTablePartitioned(t)) { tableClass += "[]"; }
            visitorTmpl.add("fieldDecls", tableClass + " " + var);
        }
        maybeDeclareDeltaTable();

        visitorTmpl.add("fieldDecls", "SRuntime " + runtimeVar());
        visitorTmpl.add("fieldDecls", "final int " + epochIdVar());
        visitorTmpl.add("fieldDecls", "final int " + ruleIdVar());
        visitorTmpl.add("fieldDecls", "TableInstRegistry " + registryVar());
        visitorTmpl.add("fieldDecls", "TablePartitionMap " + partitionMapVar());
        for (Const c:rule.getConsts()) {
            String type = MyType.javaTypeName(c);
            visitorTmpl.add("fieldDecls", "final "+type+" "+c);
        }
        declareParamVariables();

        visitorTmpl.add("fieldDecls", "int " + firstTablePartitionIdx());

        declareRemoteTablesIfAny();
        declareVisitorVars();
    }
    void declareVisitorVars() {
        for (Predicate p:rule.getBodyP()) {
            Table t = getTable(p);
            String decl = "final " + t.visitorClass() + " " + visitorVar(p);
            visitorTmpl.add("fieldDecls", decl);
        }
    }

    void declareParamVariables() {
        for (Variable v : rule.getBodyVariables()) {
            if (!v.isRealVar()) continue;
            String type = MyType.javaTypeName(v);
            visitorTmpl.add("fieldDecls", type + " " + v);
        }
        if (headP.hasFunctionParam()) {
            AggrFunction f = headP.getAggrF();
            for (Variable v : f.getReturns()) {
                String type = MyType.javaTypeName(v);
                visitorTmpl.add("fieldDecls", type + " " + v);
            }
            String aggrType=MyType.javaTypeName(f.getAggrColumnType());
            visitorTmpl.add("fieldDecls", aggrType + " " + aggrVar());
        }
    }

    void maybeDeclareDeltaTable() {
        if (!accumlDelta(rule)) { return; }

        String tableClass = deltaHeadT.className();
        visitorTmpl.add("fieldDecls", tableClass + " " + deltaTableVar());
        visitorTmpl.add("fieldDecls", tableClass+" "+deltaTableReturnVar());
    }

    void declareRemoteTablesIfAny() {
        declareRemoteBodyTablesIfAny();
        declareRemoteHeadTableIfAny();
    }

    void declareRemoteBodyTablesIfAny() {
        if (!isDistributed()) { return; }
        for (int pos : tableTransferPos()) {
            String tableCls = tableMap.get(RemoteBodyTable.name(rule, pos)).className();
            String decl = tableCls + "[] " + remoteTableVar(pos) + "=new "
                    + tableCls + "[" + machineNum() + "]";
            visitorTmpl.add("fieldDecls", decl);
        }
    }

    void declareRemoteHeadTableIfAny() {
        if (!isDistributed()) { return; }
        if (!hasRemoteRuleHead()) { return;}

        Table t = headT;
        String tableCls = tableMap.get(RemoteHeadTable.name(t, rule)).className();
        int clusterSize = SRuntimeMaster.getInst().getWorkerAddrMap().size();

        String decl = tableCls + "[] " + remoteTableVar("head") + "=new "
                            + tableCls + "[" + clusterSize + "]";
        visitorTmpl.add("fieldDecls", decl);
    }

    String deltaTableReturnVar() {
        return "ret$delta$" + headT.name();
    }

    String deltaTableVar() {
        return "delta$" + headT.name();
    }

    boolean accumlDelta(Rule r) {
        if (r.inScc()) { return true; }
        if (r.hasPipelinedRules()) {
            return true;
        }
        return false;
    }

    Table getTable(Predicate p) {
        return tableMap.get(p.name());
    }

    void generateConstructor() {
        ST code = getNewMethodTmpl(visitorName, "");
        visitorTmpl.add("methodDecls", code);

        code.add("args", "int _$epochId");
        code.add("args", "int _$ruleId");
        for (Const c:rule.getConsts()) {
            String argVar = "_$"+c;
            code.add("args", c.type.getSimpleName()+" "+argVar);
            code.add("stmts", c+"="+argVar);
        }

        for (Predicate p : rule.getBodyP()) {
            Table t = getTable(p);
            if (t instanceof GeneratedT) {
                String tableClass = t.className();
                String var = getVarName(p);
                String argVar = "_$"+var;
                code.add("args", tableClass + " " + argVar);
                code.add("stmts", var + " = " + argVar);
            }
        }

        code.add("args", "SRuntime _$runtime");
        code.add("args", "int _$firstTablePartitionIdx");

        code.add("stmts", epochIdVar()+"=_$epochId");
        code.add("stmts", ruleIdVar()+"=_$ruleId");
        code.add("stmts", runtimeVar()+"=_$runtime");
        code.add("stmts", partitionMapVar() + " = _$runtime.getPartitionMap()");
        code.add("stmts", registryVar() + "= _$runtime.getTableRegistry()");
        code.add("stmts", firstTablePartitionIdx() + " = _$firstTablePartitionIdx");

        for (Predicate p : rule.getAllP()) {
            Table t = getTable(p);
            if (t instanceof GeneratedT) { continue; }

            String var = getVarName(p);
            String tableClass = t.className();
            if (isTablePartitioned(t)) {
                tableClass+="[]";
            }
            code.add("stmts", var + " = ("+tableClass+")"+registryVar()+".getTableInstArray("+t.id()+")");
        }

        for (Predicate p:rule.getBodyP()) {
            ST alloc = getVisitor(p);
            alloc.add("var", visitorVar(p));
            code.add("stmts", alloc);
        }
    }
    ST getVisitor(Predicate p) {
        ST visitor = visitorMap.get(p);
        if (visitor == null) {
            visitor = tmplGroup.getInstanceOf("newVisitor");
            Table t = getTable(p);
            visitor.add("visitorClass", t.visitorClass());
            visitorMap.put(p, visitor);
        }
        return visitor;
    }

    void generateMethods() {
        generateIdGetters();
        generateRunMethod();
        generateVisitMethods();
        generateRemoteTableMethods();
        generateDeltaTableMethods();
        generateToString();
    }

    void generateToString() {
        ST code = getNewMethodTmpl("toString", "String");
        String ruleStr = StringEscapeUtils.escapeJava(rule.toString());
        code.add("stmts", "String str=\"" + ruleStr + " epoch:\"+"+epochIdVar());
        code.add("ret", "return str");
        visitorTmpl.add("methodDecls", code);
    }

    void generateRemoteTableMethods() {
        genGetRemoteTables();
    }

    String nullifyRemoteTableMethod(Object suffix) {
        String method = "nullifyRemoteTable_";
        if (suffix != null) {
            method += suffix;
        }
        return method;
    }
    String getRemoteTableMethod(Object suffix) {
        String method = "getRemoteTable_";
        if (suffix != null)
            method += suffix;
        return method;
    }

    void genGetRemoteTables() {
        if (hasRemoteRuleBody()) {
            genGetRemoteBodyTable();
        }
        if (hasRemoteRuleHead()) {
            genGetRemoteHeadTable();
        }
    }

    void generateDeltaTableMethods() {
        genGetDeltaTableArray();
        genGetDeltaTable();
    }

    void genGetDeltaTableArray() {
        ST method = getNewMethodTmpl("getDeltaTables", "TableInst[]");
        visitorTmpl.add("methodDecls", method);
        genGetDeltaTableArrayReally(method);
    }

    void genGetDeltaTableArrayReally(ST method) {
        if (accumlDelta(rule)) {
            method.add("stmts", "return new TableInst[]{"+deltaTableReturnVar()+"}");
        } else {
            method.add("stmts", "return null");
        }
    }

    void genGetDeltaTable() {
        if (!accumlDelta(rule)) return;

        ST method = getNewMethodTmpl("getDeltaTable", deltaHeadT.className());
        visitorTmpl.add("methodDecls", method);

        genGetDeltaTableReally(method, deltaTableVar());
    }

    void genGetDeltaTableReally(ST method, String deltaVar) {
        genGetDeltaTableReallyWithPriority(method, deltaVar, deltaTableReturnVar(), "0");
    }

    void genGetDeltaTableReallyWithPriority(ST method, String deltaVar, String deltaRetVar, String priority) {
        ST if_ = tmplGroup.getInstanceOf("if");
        method.add("stmts", if_);
        if_.add("cond", deltaVar + "==null");
        String deltaCls = deltaHeadT.className();
        String alloc;
        if (hasPipelinedRules())
            alloc="("+deltaCls+")"+TmpTablePool_get()+"Small("+deltaCls+".class)";
        else alloc="("+deltaCls+")"+TmpTablePool_get()+"(getWorkerId(),"+deltaCls+".class,"+priority+")";

        if_.add("stmts", deltaVar+"="+alloc);
        if_.add("stmts", deltaRetVar+"="+deltaVar+".isEmpty()?"+deltaVar+":null");

        if_ = tmplGroup.getInstanceOf("if");
        method.add("stmts", if_);

        if_.add("cond", deltaVar+".vacancy()==0");
        if (hasPipelinedRules()) {
            addPipeliningCode(if_, deltaVar);
            if_.add("stmts", deltaVar+".clear()");
            if_.add("stmts", "TmpTablePool.free(getWorkerId(), "+deltaVar+")");
            if_.add("stmts", deltaVar+"="+alloc);
            if_.add("stmts", deltaRetVar+"="+deltaVar);
        } else {
            String cond= "if("+deltaRetVar+"!=null)";
            if_.add("stmts", cond+" getWorker().addTasksForDelta(getRuleId(),"+deltaVar+", "+priority+")");
            if_.add("stmts", deltaVar+"="+alloc);
            if_.add("stmts", deltaRetVar+"="+deltaVar+".isEmpty()?"+deltaVar+":null");
        }
        method.add("ret", "return " + deltaVar);
    }
    void addPipeliningCode(ST code, String deltaVar) {
        String deltaRulesArray=runtimeVar()+".getRuleMap(getRuleId()).getDeltaRules(getRuleId()).toArray()";
        ST forEachDepRule = tmplGroup.getInstanceOf("forEach");
        code.add("stmts", deltaVar+".setReuse(false)");

        code.add("stmts", forEachDepRule);
        forEachDepRule.add("set", deltaRulesArray);
        forEachDepRule.add("elem", "int _$pr");

        ST forEachVisitor = tmplGroup.getInstanceOf("forEach");
        forEachDepRule.add("stmts", forEachVisitor);
        String visitors = runtimeVar()+".getJoinerBuilder(_$pr).getNewJoinerInst(_$pr, new TableInst[]{"+deltaVar+"})";
        forEachVisitor.add("set", visitors);
        forEachVisitor.add("elem", "IVisitor _$v");
        forEachVisitor.add("stmts", "_$v.setWorker(getWorker())");
        forEachVisitor.add("stmts", "try { _$v.run(); } catch (SocialiteFinishEval e) {}");

        code.add("stmts", deltaVar+".setReuse(true)");
    }
    boolean hasPipelinedRules() {
        return rule.hasPipelinedRules();
    }

    boolean isRemoteHeadFirstT() {
        Predicate p = rule.firstP();
        Table t = tableMap.get(p.name());
        if (t instanceof RemoteHeadTable)
            return true;
        return false;
    }

    String remoteTable0MapKey(int tableId, String machineIdx) {
        return "((((long)" + machineIdx + ")<<30) |" + "(((long)" + tableId
                + ")<<1)" + "|0)";
    }

    String remoteTableMapKey(int tableId, String machineIdx) {
        return "((((long)" + machineIdx + ")<<30) |" + "(((long)" + tableId
                + ")<<1)" + "|1)";
    }

    String machineIdxFromKey(String longTypeKey) {
        return "(int)(" + longTypeKey + ">>>30)";
    }

    List<Integer> tableSendPos = null;

    List<Integer> tableTransferPos() {
        if (tableSendPos == null) {
            tableSendPos = Analysis.tableTransferPos(rule, tableMap);
        }
        return tableSendPos;
    }

    void genGetRemoteBodyTable() {
        for (int pos : tableTransferPos()) {
            RemoteBodyTable rt = (RemoteBodyTable)tableMap.get(RemoteBodyTable.name(rule, pos));
            String tableCls = rt.className();

            // generating get method
            ST method = getNewMethodTmpl(getRemoteTableMethod(pos), tableCls);
            visitorTmpl.add("methodDecls", method);
            method.add("comment", "generated by genGetRemoteBodyTable()");
            method.add("args", "int rangeOrHash");

            Predicate nextP = (Predicate) rule.getBody().get(pos);
            Table joinT = tableMap.get(nextP.name());
            String machineIdx = partitionMapVar() + ".machineIndexFor("+joinT.id()+", rangeOrHash)";
            method.add("stmts", "int _$machineIdx=" + machineIdx);
            method.add("stmts", tableCls+" _$remoteT="+remoteTableVar(pos)+"[_$machineIdx]");

            // alloc table
            ST if_= tmplGroup.getInstanceOf("if");
            method.add("stmts", if_);
            if_.add("cond", "_$remoteT==null");
            if_.add("stmts", "_$remoteT=("+tableCls+")"+TmpTablePool_get()+"(worker.id(),"+tableCls+".class)");
            if_.add("stmts", remoteTableVar(pos) + "[_$machineIdx]=_$remoteT");

            method.add("ret", "return _$remoteT");

            // generating nullify method
            method = getNewMethodTmpl(nullifyRemoteTableMethod(pos), "void");
            visitorTmpl.add("methodDecls", method);
            method.add("args", "int machineIdx");
            method.add("stmts", remoteTableVar(pos)+"[machineIdx]=null");
        }
    }

    RemoteHeadTable remoteHeadT() {
        Table t = headT;
        String name = RemoteHeadTable.name(t, rule);
        RemoteHeadTable rt = (RemoteHeadTable) tableMap.get(name);
        assert rt != null;
        return rt;
    }

    void getTableSize(ST code, String tableArrayVar, String sizeVar) {
        ST if_ = tmplGroup.getInstanceOf("if");
        code.add("stmts", if_);
        if_.add("cond", tableArrayVar + "!=null");

        ST for_ = tmplGroup.getInstanceOf("for");
        if_.add("stmts", for_);
        for_.add("init", "int i=0");
        for_.add("cond", "i<" + tableArrayVar + ".length");
        for_.add("inc", "i++");
        if_ = tmplGroup.getInstanceOf("if");
        if_.add("cond", tableArrayVar + "[i]!=null");
        for_.add("stmts", if_);
        if_.add("stmts", sizeVar + "+=" + tableArrayVar + "[i].size()");
    }

    void genGetRemoteHeadTable() {
        RemoteHeadTable rt = remoteHeadT();
        String tableCls = rt.className();

        ST method = getNewMethodTmpl(getRemoteTableMethod("head"), tableCls);
        visitorTmpl.add("methodDecls", method);
        method.add("args", "int _$rangeOrHash");
        genGetRemoteHeadTableReally(method, rt, tableCls);

        method = getNewMethodTmpl(nullifyRemoteTableMethod("head"), "void");
        visitorTmpl.add("methodDecls", method);
        method.add("args", "int _$machineIdx");
        genNullifyRemoteHeadTableReally(method, rt, tableCls);
    }

    void genGetRemoteHeadTableReally(ST method, RemoteHeadTable rt, String tableCls) {
        // generating getRemoteTable_head (int _$rangeOrHash)
        String machineIdx = partitionMapVar()+".machineIndexFor("+rt.origId()+", _$rangeOrHash)";
        method.add("stmts", "int _$machineIdx="+machineIdx);
        String getter = tableCls+" _$remoteT="+remoteTableVar("head")+"[_$machineIdx]";
        method.add("stmts", getter);

        ST if_ = tmplGroup.getInstanceOf("if");
        method.add("stmts", if_);
        if_.add("cond", "_$remoteT==null");
        if_.add("stmts", "_$remoteT=("+tableCls+")"+TmpTablePool_get()+"(worker.id(),"+tableCls+".class)");
        if_.add("stmts", remoteTableVar("head")+"[_$machineIdx]=_$remoteT");

        method.add("ret", "return _$remoteT");
    }

    String TmpTablePool_get() {
        if (rule.firstP()==null) return "TmpTablePool.get";

        Table t=tableMap.get(rule.firstP().name());
        if (t instanceof RemoteBodyTable || t instanceof RemoteHeadTable) {
            return "TmpTablePool.__get";
        } else if (rule instanceof DeltaRule) {
            return "TmpTablePool._get";
        } else {
            return "TmpTablePool.get";
        }
    }
    void genNullifyRemoteHeadTableReally(ST method, RemoteHeadTable rt, String tableCls) {
        // generating nullifyRemoteTable_head (int _$machineIdx)
        String getter = tableCls+" _$remoteT="+remoteTableVar("head")+"[_$machineIdx]";
        method.add("stmts", getter);
        method.add("stmts", "assert _$remoteT!=null");
        method.add("stmts", remoteTableVar("head")+"[_$machineIdx]=null");
    }

    DeltaPredicate getFirstPifDelta() {
        if (rule.firstP() instanceof DeltaPredicate)
            return (DeltaPredicate) rule.firstP();
        return null;
        /*
         * if (iterStartP==null) return null;
         *
         * if (iterStartP instanceof DeltaPredicate) return
         * (DeltaPredicate)iterStartP; return null;
         */
    }

    void maybeSendToRemoteHead(ST code, String table,
                               String machineIdx, Object idxParam) {
        ST if_ = tmplGroup.getInstanceOf("if");
        code.add("stmts", if_);
        if_.add("cond", table+".vacancy()==0");

        ST send = if_;
        String partitionIdx="0";
        sendToRemoteHead(send, table, partitionIdx, machineIdx);
    }

    void sendToRemoteHead(ST code, String table, String partitionIdx, String machineIdx) {
        code.add("stmts", "SRuntime _$rt=SRuntimeWorker.getInst()");
        code.add("stmts", "RuleMap _$rm=_$rt.getRuleMap(getRuleId())");
        code.add("stmts", "int _$depRuleId=_$rm.getRemoteHeadDep(getRuleId())");
        code.add("stmts", "EvalWithTable _$cmd=new EvalWithTable("+epochIdVar()+", _$depRuleId,"+table+","+partitionIdx+")");
        String send = "boolean _$reuse = _$rt.sender().send("+machineIdx+","+"_$cmd)";
        code.add("stmts", send);
        code.add("stmts", "if(!_$reuse)"+nullifyRemoteTableMethod("head")+"("+machineIdx+")");
    }

    void maybeSendToRemoteBody(ST code, String table, Predicate joinP) {
        Table t = tableMap.get(joinP.name());
        ST if_= tmplGroup.getInstanceOf("if");
        code.add("stmts", if_);
        if_.add("cond", table+".size()=="+table+".capacity()");

        ST send = if_;
        String machineIdx = partitionMapVar()+".machineIndexFor("+t.id()+","+joinP.first()+")";
        sendToRemoteBody(send, table, machineIdx, ""+joinP.getPos());
    }
    void maybeBroadcastToRemoteBody(ST code, String table, Predicate joinP) {
        Table t = tableMap.get(joinP.name());
        ST if_= tmplGroup.getInstanceOf("if");
        code.add("stmts", if_);
        if_.add("cond", table+".vacancy()==0");

        ST send = if_;
        broadcastToRemoteBody(send, table, ""+joinP.getPos());
    }

    void broadcastToRemoteBody(ST code, String table, String pos) {
        code.add("stmts", "SRuntime _$rt=SRuntimeWorker.getInst()");
        code.add("stmts", "RuleMap _$rm=_$rt.getRuleMap(getRuleId())");
        code.add("stmts", "int _$depRuleId=_$rm.getRemoteBodyDep(getRuleId(),"+pos+")");
        code.add("stmts", "EvalWithTable _$cmd=new EvalWithTable("+epochIdVar()+",_$depRuleId,"+table+",0)");
        String send = "boolean _$reuse = _$rt.sender().send(-1, _$cmd)";
        code.add("stmts", send);
        code.add("stmts", "if(!_$reuse)"+nullifyRemoteTableMethod(pos)+"(0)");
    }
    void sendToRemoteBody(ST code, String table, String machineIdx, String pos) {
        code.add("stmts", "SRuntime _$rt=SRuntimeWorker.getInst()");
        code.add("stmts", "RuleMap _$rm=_$rt.getRuleMap(getRuleId())");
        code.add("stmts", "int _$depRuleId=_$rm.getRemoteBodyDep(getRuleId(),"+pos+")");
        code.add("stmts", "EvalWithTable _$cmd=new EvalWithTable("+epochIdVar()+",_$depRuleId,"+table+",0)");
        String send = "boolean _$reuse = _$rt.sender().send("+machineIdx+", _$cmd)";
        code.add("stmts", send);
        code.add("stmts", "if(!_$reuse)"+nullifyRemoteTableMethod(pos)+"("+machineIdx+")");
    }

    void generateIdGetters() {
        ST getter = getNewMethodTmpl("getRuleId", "int");
        getter.add("stmts", "return " + ruleIdVar());
        visitorTmpl.add("methodDecls", getter);

        getter = getNewMethodTmpl("getEpochId", "int");
        getter.add("stmts", "return " + epochIdVar());
        visitorTmpl.add("methodDecls", getter);
    }

    String invokeCombinedIterate(Predicate p1, Predicate p2) {
        String invoke = "combined_iterate(";
        Table t1 = getTable(p1);
        Table t2 = getTable(p2);
        String v1 = getVarName(p1);
        String v2 = getVarName(p2);
        if (p1.getPos() == 0) {
            v1 += "["+ firstTablePartitionIdx()+"]";
            v2 += "["+ firstTablePartitionIdx()+"]";
            invoke += concatCommaSeparated(v1, v2)+")";
            return invoke;
        } else {
            ST for_ = tmplGroup.getInstanceOf("for");
            for_.add("init", "int _$i=0");
            for_.add("cond", "_$i<"+partitionMapVar()+".partitionNum("+t1.id()+")");
            for_.add("inc", "_$i++");
            v1 += "[_$i]";
            v2 += "[_$i]";
            invoke += concatCommaSeparated(v1, v2)+")";
            for_.add("stmts", invoke);
            return for_.render();
        }

    }

    void generateRunMethod() {
        ST run = getNewMethodTmpl("run", "void");
        visitorTmpl.add("methodDecls", run);
        run.add("stmts", "_run()");

        ST _run = getNewMethodTmpl("_run", "boolean");
        visitorTmpl.add("methodDecls", _run);
        _run.add("ret", "return true");

        ST code = _run;
        for (Object o : rule.getBody()) {
            if (o instanceof Expr) {
                code = insertExprCode(code, (Expr) o);
            } else if (o instanceof Predicate) {
                Predicate p = (Predicate) o;
                if (allVarsResolvedOrDontcare(p)) {
                    code = returnIfNotContains(code, p);
                } else if (canCombinedIterate(p)) {
                    Predicate p2 = (Predicate)rule.getBody().get(p.getPos()+1);
                    genCombinedIterate(p, p2);
                    code.add("stmts", invokeCombinedIterate(p, p2));
                    iterStartP = p;
                    break;
                } else {
                    Table t = getTable(p);
                    String tableVar = getVarName(p);
                    if (isTablePartitioned(t)) {
                        boolean first = rule.getBody().get(0).equals(p);
                        if (first) {
                            tableVar += "[" + firstTablePartitionIdx() + "]";
                        } else if (isResolved(p, p.first())) {
                            tableVar += "[" + partitionIdxGetter(t, p.first()) + "]";
                        } else {
                            ST for_ = tmplGroup.getInstanceOf("for");
                            for_.add("init", "int _$i=0");
                            for_.add("cond", "_$i<"+partitionMapVar()+".partitionNum("+t.id()+")");
                            for_.add("inc", "_$i++");
                            code.add("stmts", for_);
                            code = for_;
                            tableVar += "[_$i]";
                        }
                    }
                    iterStartP = p;
                    insertIterateCodeInRun(code, tableVar);
                    break;
                }
            } else { assert false:"Expecting Expr or Predicate, but got:"+o; }
        }
        if (iterStartP == null) {
            if (headTableLockNeeded()) {
                String _headTable = headTableVar()+headTablePartition();
                ST withlock = CodeGen.withLock(_headTable, headP.first());
                code.add("stmts", withlock);
                code = withlock;
            }
            insertUpdateAccumlOrPipelining(code);
            genRunMethodFini(run);
            return;
        }

        genRunMethodFini(run);
    }

    void genRunMethodFini(ST code) {
        finishSendRemoteTablesIfAny(code);
        freeRemoteTables(code);
    }

    void freeRemoteTables(ST code) {
        if (hasRemoteRuleHead()) {
            code.add("stmts", "TmpTablePool.free(worker.id(),"+remoteTableVar("head")+")");
        }
        if (hasRemoteRuleBody()) {
            for (int pos : tableTransferPos()) {
                code.add("stmts", "TmpTablePool.free(worker.id(),"+remoteTableVar(pos)+")");
            }
        }
    }

    void finishSendRemoteRuleHead(ST code) {
        ST for_ = tmplGroup.getInstanceOf("for");
        code.add("stmts", for_);
        // going over machines (_$i for machine index)
        for_.add("init", "int _$i=0");
        for_.add("cond", "_$i<" + remoteTableVar("head") + ".length");
        for_.add("inc", "_$i++");
        for_.add("stmts", "TmpTableInst _$t=" + remoteTableVar("head") + "[_$i]");
        ST if_ = tmplGroup.getInstanceOf("if");
        for_.add("stmts", if_);
        if_.add("cond", "_$t!=null && _$t.size()>0");
        sendToRemoteHead(if_, "_$t", "0", "_$i");
    }

    void finishSendRemoteRuleBody(ST code) {
        for (int pos: tableTransferPos()) {
            ST for_ = tmplGroup.getInstanceOf("for");
            code.add("stmts", for_);
            Predicate p=(Predicate)rule.getBody().get(pos);
            if (requireBroadcast(p)) {
                for_.add("init", "int _$i=0");
                for_.add("cond", "_$i<1");
                for_.add("inc", "_$i++");
                for_.add("stmts", "if ("+remoteTableVar(pos)+"[0]==null) continue");
                broadcastToRemoteBody(for_, remoteTableVar(pos)+"[0]", ""+pos);
            } else {
                // going over machines (_$i for machine-index)
                for_.add("init", "int _$i=0");
                for_.add("cond", "_$i<" + remoteTableVar(pos) + ".length");
                for_.add("inc", "_$i++");

                //String tableCls = tableMap.get(RemoteBodyTable.name(rule, pos)).className();
                for_.add("stmts", /*tableCls +*/"TmpTableInst _$t=" + remoteTableVar(pos) + "[_$i]");
                ST if_ = tmplGroup.getInstanceOf("if");
                for_.add("stmts", if_);
                if_.add("cond", "_$t!=null && _$t.size()>0");
                sendToRemoteBody(if_, "_$t", "_$i", "" + pos);
            }
        }
    }

    String remoteBodyTablePos(String table) {
        String result = "";
        for (int pos : tableTransferPos()) {
            RemoteBodyTable rt = (RemoteBodyTable)tableMap.get(RemoteBodyTable.name(rule, pos));
            result += "(" + table + " instanceof " + rt.className() + ")?"
                    + pos + ":\n\t\t";
        }
        result += "-1";
        return result;
    }

    void finishSendRemoteTablesIfAny(ST code) {
        if (hasRemoteRuleHead()) {
            finishSendRemoteRuleHead(code);
        }
        if (hasRemoteRuleBody()) {
            finishSendRemoteRuleBody(code);
        }
    }

    //XXX: should check the tables in the SCC, and see if
    //     any of them has group-by with 2+ args or non-primitive arg.
    boolean bodyTableLockRequired() {
        if (headT instanceof DeltaTable) {
            DeltaTable deltaHeadT = (DeltaTable)headT;
            Table t = deltaHeadT.origT();
            assert rule.getBodyP().size()==1;
            assert t.name().equals(iterStartP.name());
            return true;
        } else return false;
    }

    void insertIterateCodeInRun(ST run, String tableVar) {
        ST code = run;
        if (headTableLockAtStart()) {
            assert iterStartP == rule.firstP();
            String _headTable = headTableVar()+headTablePartition();
            ST withlock = CodeGen.withLock(_headTable);
            code.add("stmts", withlock);
            code = withlock;
        }

        String invokeIter;
        Set<Variable> resolvedVars = resolvedVarsArray[iterStartP.getPos()];
        if (updateFromRemoteHeadT()) {
            invokeIter = tableVar+".iterate("+visitorVar(rule.firstP())+")";
            code.add("stmts", invokeIter);
        } else {
            invokeIter = tableVar+invokeIterate(iterStartP, resolvedVars);
            code.add("stmts", invokeIter);
        }
    }

    // see also {@link QueryCodeGen#getIndexByCols()}
    TIntArrayList getIndexByCols(Predicate p) {
        // returns the columns used for the iterate_by_# method
        ArrayList<Column> resolvedIdxCols = getResolvedIndexCols(p);
        TIntArrayList idxbyCols = new TIntArrayList(4);
        if (resolvedIdxCols.size()>=1)
            idxbyCols.add(resolvedIdxCols.get(0).getAbsPos());
        if (resolvedIdxCols.size()<=1) return idxbyCols;

        Table t = getTable(p);
        List<ColumnGroup> colGroups = t.getColumnGroups();
        if (colGroups.size()==1) return idxbyCols;

        int nest=1;
        for (ColumnGroup g:colGroups.subList(1, colGroups.size())) {
            if (nest > 3) break;
            if (g.first().isIndexed()) {
                Column idxCol = g.first();
                if (resolvedIdxCols.contains(idxCol))
                    idxbyCols.add(idxCol.getAbsPos());
            } else { break; }
            nest++;

        }
        return idxbyCols;
    }
    ArrayList<Column> getResolvedIndexCols(Predicate p) {
        Table t = getTable(p);
        Object[] params = p.inputParams();
        ArrayList<Column> idxCols = new ArrayList<Column>();
        Set<Variable> resolvedVars = resolvedVarsArray[p.getPos()];
        for (int i=0; i<params.length; i++) {
            if (isConstOrResolved(resolvedVars, params[i])) {
                if (t.getColumn(i).isIndexed()) {
                    idxCols.add(t.getColumn(i));
                }
                /*if (t.getColumn(i).isSorted()) {
                    idxCols.add(t.getColumn(i));
                }*/
            }
        }
        return idxCols;
    }
    boolean isOutmostIdxColResolved(Predicate p) {
        return getOutmostResolvedIdxCol(p) >= 0;
    }
    int getOutmostResolvedIdxCol(Predicate p) {
        Table t = getTable(p);
        if (!t.hasNesting())
            return -1;
        ColumnGroup outmostGroup = t.getColumnGroups().get(0);
        ArrayList<Column> resolvedIdxCols = getResolvedIndexCols(p);
        for (Column c:resolvedIdxCols) {
            if (c.position() >= outmostGroup.startIdx() &&
                    c.position() <= outmostGroup.endIdx())
                return c.position();
        }
        return -1;
    }

    boolean isIndexColResolved(Predicate p) {
        if (getResolvedIndexCols(p).size()==0)
            return false;
        return true;
    }

    boolean isConstOrResolved(Set<Variable> resolvedVars, Object varOrConst) {
        if (varOrConst instanceof Variable) {
            Variable v = (Variable) varOrConst;
            return resolvedVars.contains(v);
        }
        return true;
    }

    boolean isConstOrDontCare(Object o) {
        if (o instanceof Variable) { return isDontCare(o); }
        else { return true; }
    }

    boolean isDontCare(Object o) {
        if (o instanceof Variable)
            if (((Variable) o).dontCare)
                return true;
        return false;
    }

    boolean allVarsResolvedOrDontcare(Predicate p) {
        Set<Variable> resolvedVars = resolvedVarsArray[p.getPos()];
        for (Object param : p.inputParams()) {
            if (!isDontCare(param) && !isConstOrResolved(resolvedVars, param))
                return false;
        }
        return true;
    }

    int getPosInParams(Predicate p, Variable v) {
        Object params[] = p.inputParams();
        for (int i = 0; i < params.length; i++) {
            if (params[i].equals(v))
                return i;
        }
        return -1;
    }

    Column getSortedColumn(Predicate p, Variable v) {
        Table t = getTable(p);
        int pos = getPosInParams(p, v);
        if (pos >= 0) {
            Column c = t.getColumn(pos);
            if (c.isSorted()) { return c; }
        }
        return null;
    }

    boolean isSortedColumn(Predicate p, Variable v) {
        return getSortedColumn(p, v) != null;
    }

    CmpOp.CmpType cmpTypeForIteratePart(Predicate p) {
        Object next = rule.getBody().get(p.getPos() + 1);
        Op op = ((Expr) next).root;
        CmpOp cmpOp = (CmpOp) op;

        Object lhs = cmpOp.getLHS();
        Object rhs = cmpOp.getRHS();

        Object val = cmpValForIteratePart(p);
        if (val.equals(rhs))
            return cmpOp.cmpType();
        else
            return cmpOp.cmpType().reverse();
    }
    Object cmpValForIteratePart(Predicate p) {
        Object next = rule.getBody().get(p.getPos() + 1);
        Op op = ((Expr) next).root;
        CmpOp cmpOp = (CmpOp) op;

        Object lhs = cmpOp.getLHS();
        Object rhs = cmpOp.getRHS();
        int sortCol = idxForIteratePart(p);
        Variable v = (Variable) p.inputParams()[sortCol];
        if (v.equals(lhs)) { return rhs; }
        else { return lhs; }
    }

    int idxForIteratePart(Predicate p) {
        Object next = rule.getBody().get(p.getPos() + 1);
        Op op = ((Expr) next).root;
        CmpOp cmpOp = (CmpOp) op;

        Table t = getTable(p);
        Object lhs = cmpOp.getLHS();
        Object rhs = cmpOp.getRHS();
        if (lhs instanceof Variable) {
            Variable v = (Variable) lhs;
            Column c = getSortedColumn(p, v);
            if (c != null && c.isSorted())
                return c.getAbsPos();
        }
        if (rhs instanceof Variable) {
            Variable v = (Variable) rhs;
            Column c = getSortedColumn(p, v);
            assert (c != null && c.isSorted());
            return c.getAbsPos();
        }
        Assert.impossible("JoinerCodeGen.idxForIteratePart(): Should not reach here!");
        return -1;
    }

    boolean hasCmpNext(Predicate p) {
        if (getNextCmpOp(p)==null) return false;
        return true;
    }
    CmpOp getNextCmpOp(Predicate p) {
        if (p.getPos() == rule.getBody().size() - 1)
            return null;
        Object next = rule.getBody().get(p.getPos() + 1);
        if (!(next instanceof Expr))
            return null;
        Op op = ((Expr) next).root;
        if (!(op instanceof CmpOp))
            return null;
        return (CmpOp)op;
    }

    int getNestingLevel(Predicate p, int col) {
        Table t = getTable(p);
        int level=0;
        for (int pos:t.nestPos()) {
            if (pos <= col)
                level++;
        }
        return level;
    }
    int getNestingLevel(Predicate p, Column c) {
        return getNestingLevel(p, c.position());
    }
    boolean iteratePart(Predicate p, Set<Variable> resolved) {
        if (!hasCmpNext(p)) return false;

        CmpOp cmpOp = getNextCmpOp(p);
        if (cmpOp.getOp().equals("!="))
            return false; // not worth extra effort, so don't optimize for this.
        if (cmpOp.getOp().equals("=="))
            return false; // should be handled by iterate_by

        Object lhs = cmpOp.getLHS();
        Object rhs = cmpOp.getRHS();
        if (lhs instanceof Variable) {
            Variable v = (Variable) lhs;
            return canBinarySearch(p, v, rhs, resolved);
        }
        if (rhs instanceof Variable) {
            Variable v = (Variable) rhs;
            return canBinarySearch(p, v, lhs, resolved);
        }
        return false;
    }
    boolean canBinarySearch(Predicate p, Variable v, Object cmp, Set<Variable> resolved) {
        if (!isSortedColumn(p, v)) { return false; }
        Column sortedCol = getSortedColumn(p, v);
        if (getNestingLevel(p, sortedCol) >= 2) {
            // nesting level too deep, see table code templates (e.g. DynamicNestedTable.stg).
            return false;
        }
        return isConstOrResolved(resolved, cmp) || isInPrevColGroup(p, v, cmp);
    }

    boolean isInPrevColGroup(Predicate p, Variable v, Object param) {
        Table t = getTable(p);
        List<ColumnGroup> colGroups = t.getColumnGroups();
        int colGroupIdxOfV = -1;
        int colGroupIdxOfParam = -1;
        for (ColumnGroup cg : colGroups) {
            for (int i = cg.startIdx(); i <= cg.endIdx(); i++) {
                if (p.inputParams()[i].equals(v))
                    colGroupIdxOfV = cg.startIdx();
                if (p.inputParams()[i].equals(param))
                    colGroupIdxOfParam = cg.startIdx();
            }
        }
        assert (colGroupIdxOfV >= 0 && colGroupIdxOfParam >= 0);
        if (colGroupIdxOfParam < colGroupIdxOfV)
            return true;
        return false;
    }

    Column getSortedColumn(Table t) {
        for (Column c:t.getColumns()) {
            if (c.isSorted()) {
                return c;
            }
        }
        return null;
    }
    Variable getSortedColumnVar(Predicate p, Table t) {
        Column sortedCol = getSortedColumn(t);
        if (sortedCol == null) { return null; }
        Param param = p.inputParams()[sortedCol.getAbsPos()];
        if (param instanceof Variable) {
            return (Variable)param;
        } else {
            return null;
        }
    }
    boolean canCombinedIterate(Predicate p) {
        // returns true if p and the next predicate
        // can be iterated together using combined iterator.
        // Currently we require the sorted column to be the 1st column.
        if (true) return false;

        Table t1 = getTable(p);
        Variable v1 = getSortedColumnVar(p, t1);
        if (v1 == null) { return false; }
        if (!t1.getColumn(0).isSorted()) {
            return false;
        }

        int nextPos = p.getPos()+1;
        List body = rule.getBody();
        if (nextPos >= body.size()) { return false; }

        Object next = body.get(nextPos);
        if (!(next instanceof Predicate)) { return false; }

        Predicate nextP = (Predicate)next;
        Table t2 = getTable(nextP);
        Variable v2 = getSortedColumnVar(nextP, t2);
        if (v2 == null) { return false; }
        if (!t2.getColumn(0).isSorted()) {
            return false;
        }
        return v1.equals(v2);
    }

    void genCombinedIterate_arr_and_arr(Table t1, Table t2, Predicate p1, Predicate p2) {
        TableType tt1, tt2;
        if (t1.hasNesting()) { tt1 = TableType.ArrayNestedTable; }
        else { tt1 = TableType.ArrayTable; }
        if (t2.hasNesting()) { tt2 = TableType.ArrayNestedTable; }
        else { tt2 = TableType.ArrayTable; }

        ST template = getCombinedIterte(tt1, tt2);
        template.add("name1", t1.className());
        template.add("name2", t2.className());
        template.add("isNested1", t1.hasNesting());
        template.add("isNested2", t2.hasNesting());
        for (Column c:t1.getColumns()) {
            template.add("columns1", c);
        }
        for (Column c:t2.getColumns()) {
            template.add("columns2", c);
        }

        template.add("visitor1", visitorVar(p1));
        template.add("visitor2", visitorVar(p2));

        visitorTmpl.add("methodDecls", template.render());
    }

    void genCombinedIterate_arr_and(Table t1, Predicate p1, Table t2, Predicate p2) {
        if (t2 instanceof ArrayTable) {
            genCombinedIterate_arr_and_arr(t1, t2, p1, p2);
        } else { // OtherTable
            throw new AssertionError("genCombinedIterate: unsupported types:"+t1+","+t2);
        }
    }
    void genCombinedIterate(Predicate p1, Predicate p2) {
        Table t1 = getTable(p1);
        Table t2 = getTable(p2);
        if (t1 instanceof ArrayTable) {
            genCombinedIterate_arr_and(t1, p1, t2, p2);
        } else {

            throw new AssertionError("genCombinedIterate: unsupported types:"+t1+","+t2);
        }

    }

    void generateVisitMethods() {
        if (iterStartP == null) return;

        Set<Variable>[] resolved = resolvedVarsArray;
        ST code = null;

        List body = rule.getBody();
        Predicate prevp = null;
        for (int i = iterStartP.getPos(); i < body.size(); i++) {
            Object o = body.get(i);
            if (o instanceof Predicate) {
                Predicate p = (Predicate) o;
                if (allVarsResolvedOrDontcare(p)) {
                    code = returnIfNotContains(code, (Predicate) o);
                } else if (canCombinedIterate(p)) {
                    Predicate p2 = (Predicate) body.get(i+1);
                    if (p != iterStartP) {
                        genCombinedIterate(p, p2);
                    }
                    genVisitMethodFor(p, resolved[i]);
                    i++;
                    code = genVisitMethodFor(p2, resolved[i]);
              } else {
                    boolean added = insertContinuationCode(code, prevp, p, resolved[i]);
                    code = genVisitMethodFor(p, resolved[i]);
                    prevp = p;
                    if (added && iteratePart(p, resolved[i])) {
                        i++;
                    }
                }
            } else if (o instanceof Expr) {
                Expr e = (Expr) o;
                assert (code != null);
                code = insertExprCode(code, e);
            } else
                Assert.impossible();
        }
        insertUpdateAccumlOrPipelining(code);
    }

    boolean hasRemoteRuleBody() {
        if (!isDistributed()) return false;
        return Analysis.hasRemoteRuleBody(rule, tableMap);
    }

    boolean hasRemoteRuleHead() {
        if (!isDistributed()) return false;
        return Analysis.hasRemoteRuleHead(rule, tableMap);
    }

    void insertUpdateAccumlOrPipelining(ST code) {
        code.add("stmts", "boolean " + isUpdatedVar() + "=false");
        if (hasRemoteRuleHead()) {
            ST ifLocal = genAccumlRemoteHeadTable(code);
            code = ifLocal;
        }
        ST ifUpdated = genUpdateCode(code);
        code = ifUpdated;
        genAccumlDeltaIfAny(code);
    }

    String getRemoteBodyTable(int pos, Object rangeOrHash) {
        Object param = isInt(rangeOrHash)? rangeOrHash:"HashCode.get("+rangeOrHash+")";
        return getRemoteTableMethod(pos)+"("+param+")";
    }

    String getRemoteHeadTable(Object machineIdx) {
        Object param = isInt(machineIdx)? machineIdx:"HashCode.get("+machineIdx+")";
        return getRemoteTableMethod("head")+"("+param+")";
    }

        boolean isInt(Object o) {
            return MyType.javaType(o).equals(int.class);
        }
    ST genAccumlRemoteHeadTable(ST code) {
        Table t = headT;
        ST ifLocalElse = tmplGroup.getInstanceOf("ifElse");
        code.add("stmts", ifLocalElse);
        ifLocalElse.add("cond", partitionMapVar()+".isLocal("+t.id()+","+headP.first()+")");
        ST ifLocal = tmplGroup.getInstanceOf("simpleStmts");
        ifLocalElse.add("stmts", ifLocal);

        RemoteHeadTable rt = remoteHeadT();
        String tableCls = rt.className();
        ifLocalElse.add("elseStmts", tableCls + " _$remoteT");
        ifLocalElse.add("elseStmts", "int _$machineIdx=" + partitionMapVar()+
                            ".machineIndexFor("+rt.origId()+","+headP.first()+")");
        ifLocalElse.add("elseStmts", "_$remoteT="+getRemoteHeadTable(headP.first()));

        ST stmts = tmplGroup.getInstanceOf("simpleStmts");
        ifLocalElse.add("elseStmts", stmts);

        stmts.add("stmts", insertArgs("_$remoteT", headP.inputParams()));

        ST maybeSend = tmplGroup.getInstanceOf("simpleStmts");
        ifLocalElse.add("elseStmts", maybeSend);
        maybeSendToRemoteHead(maybeSend, "_$remoteT", "_$machineIdx", headP.first());
        return ifLocal;
    }

    boolean requireBroadcast(Predicate p) {
        if (p instanceof PrivPredicate) { return false; }
        if (isResolved(p, p.first())) {
            return false;
        } else {
            return true;
        }
    }
    ST genAccumlRemoteBodyTable(ST code, Predicate p) {
        assert tableTransferPos().contains(p.getPos());

        if (requireBroadcast(p)) {
            return genBroadcastAccumlRemoteBodyTable(code, p);
        }
        Table joinT = tableMap.get(p.name());
        ST ifElse = tmplGroup.getInstanceOf("ifElse");
        code.add("stmts", ifElse);
        ifElse.add("cond", partitionMapVar()+".isLocal(" + joinT.id()+", "+p.first() + ")");
        ST ifLocal = tmplGroup.getInstanceOf("simpleStmts");
        ifElse.add("stmts", ifLocal);

        RemoteBodyTable rt = (RemoteBodyTable)tableMap.get(RemoteBodyTable.name(rule, p.getPos()));
        String tableCls = rt.className();
        ifElse.add("elseStmts", tableCls+" _$remoteT="+getRemoteBodyTable(p.getPos(), p.first()));
        ifElse.add("elseStmts", insertArgs("_$remoteT",rt.getParamVars()));

        ST maybeSend = tmplGroup.getInstanceOf("simpleStmts");
        ifElse.add("elseStmts", maybeSend);
        maybeSendToRemoteBody(maybeSend, "_$remoteT", p);
        return ifLocal;
    }
    ST genBroadcastAccumlRemoteBodyTable(ST code, Predicate p) {
        RemoteBodyTable rt = (RemoteBodyTable)tableMap.get(RemoteBodyTable.name(rule, p.getPos()));
        String tableCls = rt.className();
        code.add("stmts", tableCls+" _$remoteT="+getRemoteBodyTable(p.getPos(), "0"));
        code.add("stmts", insertArgs("_$remoteT",rt.getParamVars()));
        ST maybeSend = tmplGroup.getInstanceOf("simpleStmts");
        code.add("stmts", maybeSend);
        maybeBroadcastToRemoteBody(maybeSend, "_$remoteT", p);

        ST ifLocal = tmplGroup.getInstanceOf("simpleStmts");
        code.add("stmts", ifLocal);
        return ifLocal;
    }

    String toArgs(List params) {
        String args="";
        for (int i=0; i<params.size(); i++) {
            args += params.get(i);
            if (i!=params.size()-1) args += ", ";
        }
        return args;
    }
    void genAccumlDeltaIfAny(ST code) {
        if (!accumlDelta(rule)) { return; }

        String deltaTable = "getDeltaTable()";
        code.add("stmts", updateHeadParamsTo(deltaTable));
    }

    String headTablePartition() {
        if (!isTablePartitioned(headT)) { return ""; }

        if (updateFromRemoteHeadT()) {
                return "["+partitionIdxGetter(headT, headP.first())+"]";
        } else if (Analysis.updateParallelShard(rule, tableMap)) {
                return "["+ firstTablePartitionIdx()+"]";
        } else {
                return "["+partitionIdxGetter(headT, headP.first())+"]";
        }
    }

    // returns place-holder for if-updated code
    ST genUpdateCode(ST code) {
        ST ifUpdated = tmplGroup.getInstanceOf("simpleStmts");
        if (headT instanceof DeltaTable) {
            // For some starting rules in SCC, like DeltaFoo(a,b) :- Foo(a,b). , where Foo is in SCC
            // do nothing here, and set updatedVar true, so that delta table is updated
            code.add("stmts", isUpdatedVar()+"=true");
            return code;
        }

        String _headTable = "_$"+headTableVar();
        code.add("stmts", headT.className()+" "+_headTable+"="+headTableVar() + headTablePartition());

        ST prevCode=code;
        if (headTableLockAtEnd()) {
            ST withLock = CodeGen.withLock(_headTable, headP.first());
            code.add("stmts", withLock);
            code = withLock;
        }

        if (headP.hasFunctionParam()) {
            genAggrCode(code, _headTable, ifUpdated);
        } else {
            genInsertCode(code, _headTable, ifUpdated);
        }

        ST if_ = tmplGroup.getInstanceOf("if");
        if_.add("cond", isUpdatedVar());
        if_.add("stmts", ifUpdated);
        prevCode.add("stmts", if_);
        return ifUpdated;
    }

    String groupbyGetPos(String headTable, AggrFunction f) {
        String groupbyGetPos = headTable + ".groupby_getpos(";
        for (int i = 0; i < f.getIdx(); i++) {
            Object p = headP.inputParams()[i];
            groupbyGetPos += p;
            if (i != f.getIdx() - 1)
                groupbyGetPos += ", ";
        }
        groupbyGetPos += ")";
        return groupbyGetPos;
    }
    String groupbyDone(String headTable, AggrFunction f, String groupbyPosVar) {
        String groupbyDone = headTable + ".groupby_done(";
        for (int i = 0; i < f.getIdx(); i++) {
            Object p = headP.inputParams()[i];
            groupbyDone += p;
            groupbyDone += ", ";
        }
        groupbyDone += groupbyPosVar+")";
        return groupbyDone;
    }

    String ifThen(String cond, String then) {
         return "if" + cond + "{" + then + ";} ";
    }
    String ifThenElse(String cond, String then, String _else) {
        return "if" + cond + "{" + then + ";} "+
                "else {"+_else+";}";
    }
    void genAggrCode(ST code, String headTableVar, ST ifUpdated) {
        AggrFunction f = headP.getAggrF();

        String gbupdate = groupbyUpdate(headTableVar, f);
        code.add("stmts", isUpdatedVar()+"=" + gbupdate);
    }

    String assignVars(List lhs, List rhs) {
        assert lhs.size()==rhs.size();
        String assign = "";
        for (int i = 0; i < lhs.size(); i++) {
            assign += lhs.get(i) + "=" + rhs.get(i) + "; ";
        }
        return assign;
    }

    String insertHeadParamsTo(String tablePartition) {
        return insertHeadParamsTo(tablePartition, "");
    }
    String insertHeadParamsTo(String tablePartition, String prefix) {
        String insert = tablePartition + "."+prefix+"insert";
        insert += makeHeadParamsArgs();
        return insert;
    }
    String groupbyUpdate(String tablePartition, AggrFunction func) {
        String insert = tablePartition + "."+"groupby_update";
        insert += makeHeadParamsArgs(func.className()+".get()");
        return insert;
    }

    String updateHeadParamsTo(String tablePartition) {
        return updateHeadParamsTo(tablePartition, "", null);
    }
    String updateHeadParamsTo(String tablePartition, String prefix, String arg0) {
        String update = tablePartition + "."+prefix+"update";
        update += makeHeadParamsArgs(arg0);
        return update;
    }

    String makeHeadParamsArgs() {
        return makeHeadParamsArgs(null);
    }
    String makeHeadParamsArgs(String arg0) {
        String args = "(";
        boolean first=true;
        if (arg0 != null) {
            args += arg0;
            first=false;
        }
        Object[] params = headP.params.toArray();
        for (Object o:params) {
            if (!first) args += ", ";
            if (o instanceof Function) {
                Function f = (Function)o;
                for (Param a:f.getArgs()) {
                    args += a;
                }
            } else { args += o; }
            first = false;
        }
        return args+")";
    }

    String insertArgs(String table, Object[] args) {
        String insert = table + ".insert(";
        boolean firstParam = true;
        for (Object a:args) {
            if (!firstParam) insert += ", ";
            insert += a;
            firstParam = false;
        }
        insert += ")";
        return insert;
    }
    String insertArgs(String table, List args) {
        String insert = table + ".insert(";
        boolean firstParam = true;
        for (Object a:args) {
            if (!firstParam) insert += ", ";
            insert += a;
            firstParam = false;
        }
        insert += ")";
        return insert;
    }

    void genInsertCode(ST code, String headTableVar, ST ifUpdated) {
        String inserter = insertHeadParamsTo(headTableVar);
        code.add("stmts", isUpdatedVar()+"=" + inserter);
    }

    String partitionIdxGetter(Table t, Object val) {
        if (isSequential()) return "0";

        int id = t.id();
        if (t.isArrayTable()) {
            return partitionMapVar() + ".getRangeIndex("+t.id()+","+val+")";
        } else {
            return partitionMapVar() + ".getHashIndex("+t.id()+","+val+")";
        }
    }

    boolean isReturnType(ST method, Class type) {
        String ret = (String) method.getAttribute("type");
        if (type.getSimpleName().equals(ret))
            return true;
        else
            return false;
    }

    ST handleErrorFor(AssignOp assign, ST code) {
        assert assign.fromFunction();
        Function f = (Function)assign.arg2;

        ST tryCatch = tmplGroup.getInstanceOf("tryCatch");
        code.add("stmts", tryCatch);

        tryCatch.add("errorVar", "_$e");
        String _throw="if(_$e instanceof SociaLiteException) throw (SociaLiteException)_$e";
        tryCatch.add("catchStmts", _throw);

        String msg = "\"Error while invoking $"+f.name()+"(\"";
        for (int i=0; i<f.getArgs().size(); i++) {
            if (i!=0) msg += "+\",\"";
            Object a = f.getArgs().get(i);
            if (MyType.javaType(a).equals(String.class))
                a = "\"\\\"\"+" + a + "+\"\\\"\"";
            msg += "+"+a;
        }
        msg += "+\"), \""+"+_$e";
        msg += "+\"\"";
        tryCatch.add("catchStmts", "VisitorImpl.L.error(ExceptionUtils.getStackTrace(_$e))");
        _throw="throw new SociaLiteException("+msg+"+\", \"+_$e)";
        tryCatch.add("catchStmts", _throw);
        return tryCatch;
    }
    ST insertExprCode(ST code, Expr expr) {
        if (expr.root instanceof AssignOp) {
            AssignOp assign = (AssignOp) expr.root;
            if (assign.fromFunction()) {
                code = handleErrorFor(assign, code);
            }
            ST code2 = assign.codegen();
            code.add("stmts", code2);

            if (assign.multiRows())
                code = code2; // returning body of an iterator-loop
        } else {
            ST if_  = tmplGroup.getInstanceOf("if");
            if_.add("cond", "!" + expr.codegen().render());
            if_.add("stmts", "return true");
            code.add("stmts", if_);
        }
        return code;
    }

    boolean isSequential() {
        return ClusterConf.get().getNumWorkerThreads() == 1;
    }

    boolean isParallel() {
        return !isSequential();
    }

    boolean updateFromRemoteHeadT() {
        if (rule.getBodyP().size() == 1) {
            Table onlyT = tableMap.get(rule.firstP().name());
            if (onlyT instanceof RemoteHeadTable)
                return true;
        }
        return false;
    }

    boolean headTableLockNeeded() {
        if (headT instanceof DeltaTable) { return false; }

        return true;
    }

    boolean headTableLockAtStart() {
        if (!headTableLockNeeded())	{ return false; }
        if (!isDistributed() && Analysis.updateParallelShard(rule, tableMap)) { return true; }
        // !rule.inScc(): HACK!
        if (!isDistributed() && Analysis.isSequentialRule(rule, tableMap) &&
                !rule.inScc()) { return true; }
        return false;
    }

    // true if write lock is needed for head table insertion (per insertion)
    boolean headTableLockAtEnd() {
        if (!headTableLockNeeded()) return false;
        if (headTableLockAtStart()) return false;

        return true;
    }

    String concatCommaSeparated(Object ...args) {
        String result = "";
        for (int i=0; i<args.length; i++) {
            result += args[i];
            if (i!=args.length-1) { result += ","; }
        }
        return result;
    }

    String invokeIterate(Predicate p, Set<Variable> resolvedVars) {
        String invokeIterate = null;
        String visitor = visitorVar(p);
        if (iteratePart(p, resolvedVars)) {
            int sortedCol = idxForIteratePart(p);
            CmpOp.CmpType cmpType = cmpTypeForIteratePart(p);
            Object cmpVal = cmpValForIteratePart(p);

            String range = "new ColumnConstraints().addRange("+sortedCol+",";
            if (cmpType.greaterThan()) {
                range += cmpVal + ", null)";
            } else {
                range += "null, "+cmpVal + ")";
            }
            invokeIterate = ".iterate_by("+range+", "+visitor+")";
        } else if (isIndexColResolved(p)) {
            String by = "new ColumnConstraints()";
            Table t = getTable(p);
            Object[] params = p.inputParams();
            for (int i=0; i<params.length; i++) {
                if (isConstOrResolved(resolvedVars, params[i])) {
                    if (t.getColumn(i).isIndexed() || t.getColumn(i).isSorted()) {
                        by += ".add("+i+", "+params[i]+")";
                    }
                }
            }
            invokeIterate = ".iterate_by("+by+","+visitor+")";
        } else {
            invokeIterate = ".iterate("+visitor+")";
        }
        return invokeIterate;
    }

    // returns true if continuation inserted
    boolean insertContinuationCode(ST code, Predicate prevp, Predicate p,
            Set<Variable> resolved) {
        if (code == null) {
            assert (prevp == null);
            return false;
        }
        ST origCode=code;

        if (hasRemoteRuleBody() && tableTransferPos().contains(p.getPos())) {
            ST ifLocal = genAccumlRemoteBodyTable(code, p);
            code = ifLocal;
        }

        Table t = getTable(p);
        String invokeIterate = invokeIterate(p, resolved);

        String tableVar = getVarName(p);
        if (isTablePartitioned(t)) {
            boolean partitionSelected = Analysis.isResolved(resolvedVarsArray, p, p.first());
            if (partitionSelected) {
                tableVar += "[" + partitionIdxGetter(t, p.first()) + "]";
            } else {
                int tid = t.id();
                ST for_ = CodeGen.getVisitorST("for");
                code.add("stmts", for_);
                for_.add("init", "int _$$i=0");
                for_.add("cond", "_$$i<" + partitionMapVar() + ".partitionNum(" + tid + ")");
                for_.add("inc", "_$$i++");
                tableVar += "[_$$i]";
                code = for_;
            }
        }
        code.add("stmts", tableVar+invokeIterate);

        return true;
    }

    int getPrefixParamsForIter(Set<Variable> resolved, Predicate p) {
        Table t = getTable(p);
        if (t instanceof RemoteHeadTable) return 0;

        int[] indexed = t.indexedCols();
        if (indexed.length == 0) return 0;

        Param[] params = p.inputParams();
        int resolvedPrefix = 0;
        for (int i = 0; i < params.length; i++) {
            if (isConstOrResolved(resolved, params[i]))
                resolvedPrefix++;
            else break;
        }
        if (resolvedPrefix == 0) return 0;

        for (int i = 0; i < indexed.length; i++) {
            if (indexed[i] > resolvedPrefix - 1) {
                if (i == 0) return 0;
                else return indexed[i - 1] + 1;
            }
        }
        return indexed[indexed.length - 1] + 1;
    }

    boolean prefixResolved(Set<Variable> resolved, Object[] params, int prefix) {
        Assert.true_(params.length > prefix);
        for (int i = 0; i < prefix; i++) {
            Object p = params[i];
            if (!isConstOrResolved(resolved, p))
                return false;
        }
        return true;
    }

    boolean allDontCareVars(List params) {
        // returns true if all the params are don't-care variables
        // (if the params have constants, this returns false)
        for (Object o : params) {
            if (o instanceof Variable) {
                Variable v = (Variable) o;
                if (!v.dontCare)
                    return false;
            } else if (o instanceof Function) {
                Function f = (Function) o;
                Set<Variable> inputs = f.getInputVariables();
                for (Variable v : inputs) {
                    if (!v.dontCare)
                        return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    boolean dontCareVar(Object o) {
        if (o instanceof Variable) {
            Variable v = (Variable) o;
            if (v.dontCare)
                return true;
            else
                return false;
        } else {
            return false;
        }
    }
    boolean[] getDontcareFlags(Predicate p) {
        boolean[] dontCares=new boolean[p.inputParams().length];
        boolean dontCareExists=false;
        int i=0;
        for (Param o:p.inputParams()) {
            dontCares[i]=false;
            if (o instanceof Variable) {
                Variable v=(Variable)o;
                if (v.dontCare) {
                    dontCareExists=true;
                    dontCares[i]=true;
                }
            }
            i++;
        }
        if (dontCareExists) return dontCares;
        else return new boolean[] {};
    }

    ST returnIfNotContains(ST code, Predicate p) {
        boolean[] dontCares = getDontcareFlags(p);
        // p.isNegated() is used to generate negated filter
        return ifNotContains(code, p, "return true", dontCares);
    }
    ST ifNotContains(ST code, Predicate p, String actionStmt, boolean[] dontcares) {
        if (hasRemoteRuleBody() && tableTransferPos().contains(p.getPos())) {
            ST ifLocal = genAccumlRemoteBodyTable(code, p);
            code = ifLocal;
        }
        Table t = getTable(p);

        String contains = ".contains(";
        boolean firstParam = true;
        for (Param o : p.inputParams()) {
            if (!firstParam) contains += ", ";
            contains += o;
            firstParam = false;
        }
        if (dontcares.length>0) {
            contains += ", new boolean[]{";
            for (int i=0; i<dontcares.length; i++) {
                if (i!=0) contains += ",";
                contains += dontcares[i];
            }
            contains += "}";
        }
        contains += ")";

        String condition;
        String var = getVarName(p);
        if (isTablePartitioned(t)) {
            if (dontcares.length>0 && dontcares[0]) {
                String fallthrou=CodeGen.uniqueVar("_$fallthrou");
                code.add("stmts", "boolean "+fallthrou+"=true");

                ST for_ = tmplGroup.getInstanceOf("for");
                for_.add("init", "int _$$i=0");
                for_.add("cond", "_$$i<"+partitionMapVar()+".partitionNum("+t.id()+")");
                for_.add("inc", "_$$i++");
                code.add("stmts", for_);
                ST if2_ = CodeGen.getVisitorST("if");
                for_.add("stmts", if2_);

                var += "[_$$i]";
                condition = var+contains;
                if2_.add("cond", condition);
                if2_.add("stmts", fallthrou+"=false; break");
                if (p.isNegated()) { condition = "!"+fallthrou; }
                else { condition = fallthrou; }
            } else {
                var += "["+ partitionIdxGetter(t, p.first())+"]";
                if (p.isNegated()) condition = var+contains;
                else condition = "!" + var+contains;
            }
        } else {
            if (p.isNegated()) condition = var+contains;
            else condition = "!" + var+contains;
        }

        ST if_ = CodeGen.getVisitorST("if");
        code.add("stmts", if_);
        if_.add("cond", condition);
        if_.add("stmts", actionStmt);
        return code;
    }

    ST genVisitMethodFor(Predicate p, Set<Variable> resolved) {
        TIntArrayList idxbyCols = getIndexByCols(p);

        Table t = getTable(p);
        List<ColumnGroup> columnGroups = t.getColumnGroups();
        ST m = null;
        int numColumns = t.numColumns();
        for (ColumnGroup g : columnGroups) {
            int startCol = g.startIdx();
            int endCol = g.endIdx();

            m = getVisitMethod(p, startCol, endCol, numColumns);
            if (allDontCaresWithin(p.inputParams(), g.startIdx(), numColumns-1)) {
                m.add("ret", "return false");// stop iteration after the visit method m.
                break;
            } else {
                CodeGen.fillVisitMethodBody(m, p, startCol, endCol, resolved, idxbyCols);
            }
        }
        return m;
    }

    boolean isResolved(Predicate p, Param param) {
        Set<Variable> resolved = resolvedVarsArray[p.getPos()];
        if (param instanceof Variable) {
            return resolved.contains(param);
        }
        return true;
    }

    boolean allDontCaresWithin(Object[] params, int startidx, int endidx) {
        if (startidx > endidx)
            return false;
        for (int i = startidx; i <= endidx; i++) {
            Object p = params[i];
            if (!isConstOrDontCare(p))
                return false;
        }
        return true;
    }

    boolean isTablePartitioned(Table t) {
        return !(t instanceof GeneratedT);
    }

    public static String getVarName(String table) {
        String firstLetter = table.substring(0, 1);
        String rest = table.substring(1);
        String varName = "_" + firstLetter.toLowerCase() + rest;
        return varName;
    }

    static String visitorClassName(Rule r, Map<String, Table> tableMap) {
        Class visitorClass = CodeGenMain.visitorClass$.get(r.signature(tableMap));
        if (visitorClass!=null) {
            return visitorClass.getSimpleName();
        }
        String name = "Visitor" + r.getHead().name() + "_" + r.id();
        if (r instanceof DeltaRule) {
            DeltaRule dr=(DeltaRule)r;
            if (dr.getTheP().isHeadP()) name += "_delta_head";
            else name += "_delta"+dr.getTheP().getPos();
        }
        return name;
    }

    int machineNum() {
        return SRuntimeMaster.getInst().getWorkerAddrMap().size();
    }

    String remoteTableVar(Object suffix) {
        String var = "remoteTable_";
        if (suffix != null)
            var += suffix;
        return var;
    }

    String visitorVar(Predicate p) { return "$v"+p.getPos(); }
    String getVarName(Predicate p) {
        if (p.isHeadP()) {
            return headTableVar();
        } else {
            return "$"+getVarName(p.name()) + p.getPos();
        }
    }
    String headTableVar() { return "$headTable"; }
    String firstTablePartitionIdx() { return "$firstTablePartitionIdx"; }
    String registryVar() { return "$registry"; }
    String partitionMapVar() { return "$partitionMap"; }
    String runtimeVar() { return "$runtime"; }
    String ruleIdVar() { return "$ruleId"; }
    String epochIdVar() { return "$epochId"; }
    String isUpdatedVar() { return "$isUpdated"; }
    String aggrVar() { return "$aggrVar"; }
}