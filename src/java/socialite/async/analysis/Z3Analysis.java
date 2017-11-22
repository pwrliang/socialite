package socialite.async.analysis;

import socialite.parser.*;

import java.util.Arrays;

public class Z3Analysis {
    public static void main(String[] args) {
        new Z3Analysis();
    }

    String deltaVar;
    String aggrFunc;
    String leftVar;

    Z3Analysis() {
        String program = "Rank(int n:0..4, double rank).\n" +
                "Edge(int n:0..4, (int t)).\n" +
                "EdgeCnt(int n:0..4, int cnt).\n" +
                "Rank(Y, $sum(r) + 0.15) :- Rank(X, r1), Edge(X, Y), EdgeCnt(X, d), r = 0.85 * r1 / d.";
        Parser parser = new Parser(program);
        parser.parse(program);

        Rule recRule = null;
        for (Rule rule : parser.getRules()) {
            //simple recursive rule
            if (rule.getHead().name().equals(rule.firstP().name())) {
                recRule = rule;
                initLeftVarAndGFunc(rule);
                break;
            }
        }

        if (recRule == null)
            throw new RuntimeException("no recursive rule found");
        if (leftVar == null || aggrFunc == null)
            throw new RuntimeException("Cannot found leftVar and aggrFunc");

        String fFuncExpr = getFfunc(recRule);
        if(fFuncExpr==null)
            throw new RuntimeException("Cannot found fFunc");

        String gFuncInExpr = null;
        for (Literal literal : recRule.getBody()) {
            if (literal instanceof Expr) {
                Expr expr = (Expr) literal;
                if (expr.root instanceof AssignOp) {
                    AssignOp assignOp = (AssignOp) expr.root;
                    if (assignOp.arg2 instanceof BinOp) {
                        BinOp binOp = (BinOp) assignOp.arg2;
                        if (binOp.toString().contains(leftVar)) {
                            gFuncInExpr = preorderTraverse(binOp).replace(leftVar, aggrFunc);
                            break;
                        }
                    }
                }
            }
        }

        String expandedExpr = expandAggrFunc(aggrFunc);
        assert gFuncInExpr != null;
        String gFuncExpr = gFuncInExpr.replace(aggrFunc, expandedExpr);

        String g = "sum + 0.15";
        String f = "a*0.85";
    }



    String preorderTraverse(Object root) {
        if (root instanceof BinOp) {
            BinOp binOp = (BinOp) root;
            String left = null;
            String right = null;
            if (binOp.arg1 instanceof BinOp)
                left = preorderTraverse(binOp.arg1);
            else if (binOp.arg1 instanceof Const)
                left = ((Const) (binOp.arg1)).val + "";
            else left = binOp.arg1.toString();

            if (binOp.arg2 instanceof BinOp)
                right = preorderTraverse(binOp.arg2);
            else if (binOp.arg2 instanceof Const)
                right = ((Const) (binOp.arg2)).val + "";
            else right = binOp.arg2.toString();

            return String.format("(%s %s %s)", binOp.op, left, right);
        }
        return root.toString();
    }

    String expandAggrFunc(String aggrFunc) {
        switch (aggrFunc) {
            case "count":
            case "sum":
                return "(+ a b)";
            case "max":
                return "(ite (> a b) a b)";
            case "min":
                return "(ite (< a b) a b)";
            default:
                throw new UnsupportedOperationException(String.format("aggregate function %s is not supported", aggrFunc));
        }
    }


    void initLeftVarAndGFunc(Rule rule) {
        String[] supportedFunc = {"sum", "count", "max", "min"};

        for (Param param : rule.getHead().params) {
            if (param instanceof AggrFunction) {
                aggrFunc = ((AggrFunction) param).methodName();
            }
        }
        for (Literal literal : rule.getBody()) {
            if (literal instanceof Expr) {
                Expr expr = (Expr) literal;
                if (expr.root instanceof AssignOp) {
                    AssignOp assignOp = (AssignOp) expr.root;
                    if (assignOp.arg2 instanceof Function) {
                        Function function = (Function) assignOp.arg2;
                        boolean supported = Arrays.stream(supportedFunc).anyMatch(fname -> function.methodName().equals(fname));
                        if (supported) {
                            deltaVar = function.getArgs().get(0).toString();
                            aggrFunc = function.methodName();
                        }
                        leftVar = ((Variable) assignOp.arg1).name;
                    }
                }
            }
        }
    }

    String getFfunc(Rule rule){
        for(Literal literal:rule.getBody()){
            if (literal instanceof Expr) {
                Expr expr = (Expr) literal;
                if (expr.root instanceof AssignOp) {
                    AssignOp assignOp = (AssignOp) expr.root;
                    if (assignOp.arg1 instanceof Variable) {
                        Variable leftVar = (Variable) assignOp.arg1;
                        if(leftVar.name.equals(deltaVar))
                            return preorderTraverse(assignOp.arg2);
                    }
                }
            }
        }
        return null;
    }
}
