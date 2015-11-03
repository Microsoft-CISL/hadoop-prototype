package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static org.junit.Assert.*;

public class TestRandomTreeWalk {

  @Rule public TestName name = new TestName();

  Random r = new Random();

  @Before
  public void setSeed() {
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println(name.getMethodName() + " seed: " + seed);
  }

  @Test
  public void testRandomTreeWalkRepeat() throws Exception {
    Set<TreePath> ns = new HashSet<>();
    final long seed = r.nextLong();
    RandomTreeWalk t1 = new RandomTreeWalk(seed, 10, .1f);
    int i = 0;
    for (TreePath p : t1) {
      p.accept(i++);
      assertTrue(ns.add(p));
    }

    RandomTreeWalk t2 = new RandomTreeWalk(seed, 10, .1f);
    int j = 0;
    for (TreePath p : t2) {
      p.accept(j++);
      assertTrue(ns.remove(p));
    }
    assertTrue(ns.isEmpty());
  }

  @Test
  public void testRandomTreeWalkFork() throws Exception {
    Set<FileStatus> ns = new HashSet<>();

    final long seed = r.nextLong();
    RandomTreeWalk t1 = new RandomTreeWalk(seed, 10, .15f);
    int i = 0;
    for (TreePath p : t1) {
      p.accept(i++);
      assertTrue(ns.add(p.getFileStatus()));
    }

    RandomTreeWalk t2 = new RandomTreeWalk(seed, 10, .15f);
    int j = 0;
    ArrayList<TreeWalk.TreeIterator> iters = new ArrayList<>();
    iters.add(t2.iterator());
    while (!iters.isEmpty()) {
      for (TreeWalk.TreeIterator sub = iters.remove(iters.size() - 1);
           sub.hasNext();) {
        TreePath p = sub.next();
        if (0 == (r.nextInt() % 4)) {
          iters.add(sub.fork());
          Collections.shuffle(iters, r);
        }
        p.accept(j++);
        assertTrue(ns.remove(p.getFileStatus()));
      }
    }
    assertTrue(ns.isEmpty());
  }

  @Test
  public void testRandomRootWalk() throws Exception {
    Set<FileStatus> ns = new HashSet<>();
    final long seed = r.nextLong();
    Path root = new Path("foo://bar:4344/dingos");
    String sroot = root.toString();
    int nroot = sroot.length();
    RandomTreeWalk t1 = new RandomTreeWalk(root, seed, 10, .1f);
    int i = 0;
    for (TreePath p : t1) {
      p.accept(i++);
      FileStatus stat = p.getFileStatus();
      assertTrue(ns.add(stat));
      assertEquals(sroot, stat.getPath().toString().substring(0, nroot));
    }

    RandomTreeWalk t2 = new RandomTreeWalk(root, seed, 10, .1f);
    int j = 0;
    for (TreePath p : t2) {
      p.accept(j++);
      FileStatus stat = p.getFileStatus();
      assertTrue(ns.remove(stat));
      assertEquals(sroot, stat.getPath().toString().substring(0, nroot));
    }
    assertTrue(ns.isEmpty());
  }

}
