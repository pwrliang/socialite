package socialite.async.analysis;

import socialite.util.Assert;
import socialite.visitors.VisitorImpl;

public abstract class MyVisitorImpl extends VisitorImpl {

    public boolean visit(int a0, boolean a1) {
        Assert.not_implemented();
        return false;
    }
}
