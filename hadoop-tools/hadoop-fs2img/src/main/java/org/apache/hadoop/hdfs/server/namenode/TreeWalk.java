package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public abstract class TreeWalk implements Iterable<TreePath> {

  protected abstract Iterable<TreePath> getChildren(
      TreePath p, long id, TreeWalk.TreeIterator walk);

  public abstract TreeIterator iterator();

  public abstract class TreeIterator implements Iterator<TreePath> {

    protected final Deque<TreePath> pending;

    TreeIterator() {
      this(new ArrayDeque<TreePath>());
    }

    protected TreeIterator(Deque<TreePath> pending) {
      this.pending = pending;
    }

    public abstract TreeIterator fork();

    @Override
    public boolean hasNext() {
      return !pending.isEmpty();
    }

    @Override
    public TreePath next() {
      return pending.removeFirst();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    // need id? Should be set on TreePath
    protected void onAccept(TreePath p, long id) {
      for (TreePath k : getChildren(p, id, this)) {
        pending.addFirst(k);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("{ Treewalk=\"").append(TreeWalk.this.toString());
      sb.append(", pending=[");
      Iterator<TreePath> i = pending.iterator();
      if (i.hasNext()) {
        sb.append("\"").append(i.next()).append("\"");
      }
      while (i.hasNext()) {
        sb.append(", \"").append(i.next()).append("\"");
      }
      sb.append("]");
      sb.append(" }");
      return sb.toString();
    }

  }

}
