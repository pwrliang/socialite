"""
  Testing join operations.
"""

import socialite as s
import unittest

class TestIndexJoin(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        s.decl("Foo(int a, int b, (int c, int d)) indexby a, indexby b, indexby c.")
        s.decl("FooArr(int a:0..100, int b, (int c, int d)) indexby b, indexby c.")
        s.decl("Bar(int a, int b, int c, int d) indexby a, indexby c.")
        s.decl("BarArr(int a:0..1000, int b, int c, int d) indexby a, indexby c.")
        s.decl("Qux(int a, int b, int c, int d) indexby a, indexby c.")

        cls.initTables()

    @classmethod
    def initTables(cls):
        s.run("Bar(a,b,c,d) :- a=$range(0,1000), b=1, c=1, d=2.")
        s.run("BarArr(a,b,c,d) :- a=$range(0,1000), b=1, c=1, d=2.")
        s.run("Foo(a,b,c,d) :- a=$range(42, 43), b=1, c=1, d=2.")
        s.run("FooArr(a,b,c,d) :- a=$range(42, 43), b=1, c=1, d=2.")

    @classmethod
    def tearDownClass(cls):
        s.drop("*")

    def setUp(self):
        s.clear("Qux")

    def test_idx_join(self):
        self._testrun("Qux(a,b,c,d) :- Foo(a,b,c,d), Bar(a,b,c,d).")

    def test_idx_join2(self):
        self._testrun("Qux(a,b,c,d) :- FooArr(a,b,c,d), Bar(a,b,c,d).")

    def test_idx_join3(self):
        self._testrun("Qux(a,b,c,d) :- Foo(a,b,c,d), BarArr(a,b,c,d).")

    def test_idx_join4(self):
        self._testrun("Qux(a,b,c,d) :- FooArr(a,b,c,d), BarArr(a,b,c,d).")

    def _testrun(self, query):
        s.run(query)

        expected = [(a, 1, 1, 2) for a in xrange(42, 43)]
        result = []
        for a,b,c,d in s.query("Qux(a,b,c,d)"):
            result.append((a,b,c,d))

        self.assertEqual(sorted(result), sorted(expected))


class TestJoin(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        s.decl("Foo(int a, int b, int c, int d).")
        s.decl("FooArr(int a:0..100, int b, int c, int d).")
        s.decl("Bar(int a, int b, int c, int d).")
        s.decl("BarArr(int a:0..1000, int b, int c, int d).")
        s.decl("Qux(int a, int b, int c, int d).")

        cls.initTables()

    @classmethod
    def initTables(cls):
        s.run("Bar(a,b,c,d) :- a=$range(0,1000), b=1, c=1, d=2.")
        s.run("BarArr(a,b,c,d) :- a=$range(0,1000), b=1, c=1, d=2.")
        s.run("Foo(a,b,c,d) :- a=$range(42, 43), b=1, c=1, d=2.")
        s.run("FooArr(a,b,c,d) :- a=$range(42, 43), b=1, c=1, d=2.")

    @classmethod
    def tearDownClass(cls):
        s.drop("*")

    def setUp(self):
        s.clear("Qux")

    def test_combined_iterate(self):
        self._testrun("Qux(a,b,c,d) :- Foo(a,b,c,d), Bar(a,b,c,d).")

    def test_combined_iterate2(self):
        self._testrun("Qux(a,b,c,d) :- FooArr(a,b,c,d), Bar(a,b,c,d).")

    def test_combined_iterate2(self):
        self._testrun("Qux(a,b,c,d) :- Bar(a,b,c,d), FooArr(a,b,c,d).")

    def test_combined_iterate4(self):
        self._testrun("Qux(a,b,c,d) :- FooArr(a,b,c,d), BarArr(a,b,c,d).")

    def _testrun(self, query):
        s.run(query)

        expected = [(a, 1, 1, 2) for a in xrange(42, 43)]
        result = []
        for a,b,c,d in s.query("Qux(a,b,c,d)"):
            result.append((a,b,c,d))

        self.assertEqual(sorted(result), sorted(expected))


class TestSortMergeJoin2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        s.decl("""Foo(int a, (int b)) sortby a, sortby b.""")
        s.decl("""FooArr(int a:0..200, (int b)) sortby a, sortby b.""")
        s.decl("""FooArrArr(int a:0..200, (int b:0..50)) sortby a, sortby b.""")
        s.decl("""Bar(int a, (int b)) sortby a, sortby b.""")
        s.decl("""BarArr(int a:0..200, (int b)) sortby a, sortby b.""")
        s.decl("""BarArrArr(int a:0..200, (int b:0..50)) sortby a, sortby b.""")
        s.decl("""Qux(int a, int b) indexby a.""")

        cls.initTables()

    @classmethod
    def tearDownClass(cls):
        s.drop("*")

    @classmethod
    def initTables(cls):
        s.run("Foo(a,b) :- a=$range(0,160), b=$range(42, 45).")
        s.run("FooArr(a,b) :- a=$range(0,160), b=$range(42, 45).")
        s.run("FooArrArr(a,b) :- a=$range(0,160), b=$range(42, 45).")
        s.run("Bar(a,b) :- a=$range(150,200), b=$range(42, 45).")
        s.run("BarArr(a,b) :- a=$range(150,200), b=$range(42, 45).")
        s.run("BarArrArr(a,b) :- a=$range(150,200), b=$range(42, 45).")

    def setUp(self):
        s.clear("Qux")

    def test_combined_iterate(self):
        self._testrun("Qux(a,b) :- Foo(a,b), Bar(a,b).")

    def _testrun(self, query):
        s.run(query)

        expected = [(a, b) for a in range(150, 160) for b in range(42, 45)]
        result = []
        for a,b in s.query("Qux(a,b)"):
            result.append((a,b))

        self.assertEqual(sorted(result), sorted(expected))


def main():
    unittest.main()

if __name__ == '__main__':
    main()

"""
class TestJoin(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        unittest.TestCase.__init__(self, methodName)
        `Foo(int a, int b).  
         Bar(int a, int b) sortby a.`
        `Foo(a,b) :- a=$range(0, 5), b=$range(0,3).`
        # Foo(a,b) = {(0,0), (0,1), (0,2), (1,0), ...(4, 2)}

    def setUp(self):
        `clear Bar.`

    def test_simple_insert(self):
        `clear Foo.`
        `Foo(a,b) :- a=$range(0, 1000), b=42.`
        count=0
        sum = 0
        for a,b in `Foo(a,b)`:
            count+=1
            sum += b
        self.assertEqual(count, 1000,
            "Unexpected number of tuples in Foo. Expecting # 1000 tuples, but got # "+str(count))
        self.assertEqual(sum, 42*1000,
            "Unexpected sum for 2nd column of Foo. Expecting 42000, but got # "+str(sum))

        `Foo2(int a, (int b)) indexby a, indexby b.
         Foo2(a,b) :- a=$range(0, 200), b=$range(0, 420).`
        count=0
        for a,b in `Foo2(a,b)`:
            count+=1
        self.assertEqual(count, 420*200,
            "Unexpected number of tuples in Foo2. Expecting # 420000 tuples, but got # "+str(count))
       
    def test_not_contains_with_dontcare(self):
        `Bar(a,b) :- a=$range(4, 7), b=$range(0,4), !Foo(_,b).`

        l=[]
        for a,b in `Bar(a,b)`:
            l.append((a,b))
        exp = [(4,3), (5,3), (6,3)]
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))

        `clear Bar.`
        `Bar(a,b) :- a=$range(4, 7), b=$range(0,4), !Foo(a,_).`

        l=set(`Bar(a,b)`)
        exp=set([(5,0), (5,1), (5,2), (5,3), (6,0), (6,1), (6,2), (6,3)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))

    def test_contains_with_dontcare(self):
        `Bar(a,b) :- a=$range(4, 7), b=$range(0,4), Foo(_,b).`

        l=set([])
        for a,b in `Bar(a,b)`:
            l.add((a,b))
        exp = set([(4,0), (4,1), (4,2), (5,0), (5,1), (5,2), (6,0), (6,1), (6,2)])

        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))

        `clear Bar.`
        `Bar(a,b) :- a=$range(4, 7), b=$range(0,4), Foo(a,_).`

        l=set(`Bar(a,b)`)
        exp=set([(4,0), (4,1), (4,2), (4,3)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))

    def test_not_contains(self):
        `Bar(a,b) :- a=$range(4, 7), b=$range(0,4), !Foo(a,b).`

        l=set()
        for a,b in `Bar(a,b)`:
            l.add((a,b))
        exp=set([(4,3), (5,0), (5,1), (5,2), (5,3), (6,0), (6,1), (6,2), (6,3)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))

        `clear Bar.`
        `Bar(a,b) :- a=$range(4, 7), b=$range(0,4), Foo(a,b).`

        l=set(`Bar(a,b)`)
        exp=set([(4,0), (4,1), (4,2)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))

    def test_not_contains2(self):
        `StopWords(String s).
         StopWords(a) :- a=$splitIter("a an and the of in to for with", " ").`
        `Titles(String s).
         Titles(t) :- t="k-means clustering with scikit-learn".
         Titles(t) :- t="gradient boosted regression trees in scikit-learn".
         Titles(t) :- t="know thy neighbor: an introduction to scikit-learn and k-nn".
         Titles(t) :- t="sentiment classification using scikit-learn".`
        `Words(String w, int cnt).
         Words(w, $inc(1)) :- Titles(t), w=$splitIter(t, " "), !StopWords(w).`

        _,cnt=list(`Words("scikit-learn", cnt)`)[0]
        self.assertEqual(cnt, 4)

    def test_outjoin(self):
        `Qux(int a, (int b)).`

        `Bar(a, b) :- a=$range(0, 2), b=1.
         Qux(a, b) :- Foo(a, c), Bar(b, c).`


        l=set(`Qux(a,b)`)
        exp=set([(0,0), (0,1), (1,0), (1,1), (2,0), (2,1), (3,0), (3,1), (4,0), (4,1)])

        self.assertEqual(l, exp, "Unexpected tuples in Qux(a,b). Expecting "+str(exp)+", but "+str(l))

    def test_binarySearch(self):
        `FooSorted(int a, int b) sortby a.
         FooSorted(a,b) :- a=10, b=20.
         FooSorted(a,b) :- a=11, b=21.
         FooSorted(a,b) :- a=12, b=22.
         Bar(a,b) :- FooSorted(a, b), a>=11.`
        l=set(`Bar(a,b)`)
        exp=set([(11,21), (12,22)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))
        `FooSorted2(int a, float b, long c) sortby c.
         FooSorted2(a,b,c) :- a=10, b=10.0f, c=10L.
         FooSorted2(a,b,c) :- a=11, b=11.0f, c=11L.
         FooSorted2(a,b,c) :- a=12, b=12.0f, c=12L.
         Bar2(int a, long b).
         Bar2(a,c) :- FooSorted2(a, b, c), c>=11.`
        l=set(`Bar2(a,b)`)
        exp=set([(11,11.0), (12,12.0)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar2(a,b). Expecting "+str(exp)+", but "+str(l))
 
    def test_nested_binarySearch(self):
        `FooNested1(int a, (int b)) sortby a, sortby b.
         FooNested1(a,b) :- a=10, b=20.
         FooNested1(a,b) :- a=11, b=21.
         FooNested1(a,b) :- a=11, b=22.
         FooNested1(a,b) :- a=12, b=23.
         Bar(a,b) :- FooNested1(a, b), a>=11.`

        l=set(`Bar(a,b)`)
        exp=set([(11,21), (11,22), (12,23)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))
        `clear Bar.`
        `Bar(a,b) :- FooNested1(a, b), b>=22.`
        l=set(`Bar(a,b)`)
        exp=set([(11,22), (12,23)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))

    def test_nested_binarySearch2(self):
        `FooNested2(long a, (long b, float c)) sortby a, sortby c.
         FooNested2(a,b,c) :- a=10L, b=10L, c=10.0f.
         FooNested2(a,b,c) :- a=10L, b=11L, c=11.0f.
         FooNested2(a,b,c) :- a=20L, b=20L, c=20.0f.
         FooNested2(a,b,c) :- a=20L, b=21L, c=21.0f.`

        `clear Bar.`
        `Bar((int)a,(int)b) :- FooNested2(a, b, c), c>10.0f.`
        l=set(`Bar(a,b)`)
        exp=set([(10,11), (20,20), (20, 21)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))
        
        `clear Bar.`
        `Bar((int)a,(int)b) :- FooNested2(a, b, c), b>10L.`
        l=set(`Bar(a,b)`)
        exp=set([(10,11), (20,20), (20, 21)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))
 
        # binary search with (nested) long type column with indexby
        `clear Bar.`
        `FooNested2((long)a,b,c) :- a=$range(21, 10000), b=42L, c=42.0f.`

        `Bar(20,(int)b) :- FooNested2(20L, b, c), b>10L.`
        l=set(`Bar(a,b)`)
        exp=set([(20, 20), (20, 21)])
        self.assertEqual(l, exp, "Unexpected tuples in Bar(a,b). Expecting "+str(exp)+", but "+str(l))

"""

if __name__ == '__main__':
    unittest.main()
