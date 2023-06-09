package cn.edu.thssdb.index;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Record;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.storage.BufferManager;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

public class RecordTreeIterator implements Iterator<Pair<Entry, Row>> {
  private final LinkedList<BPlusTreeNode<Entry, Record>> queue;
  private final LinkedList<Pair<Entry, Row>> buffer;

  private BufferManager bufferManager;

  public RecordTreeIterator(BPlusTree<Entry, Record> tree, Table table) {
    queue = new LinkedList<>();
    buffer = new LinkedList<>();
    if (tree.size() == 0) return;
    queue.add(tree.root);
    bufferManager = tree.bufferManager;
  }

  @Override
  public boolean hasNext() {
    return !queue.isEmpty() || !buffer.isEmpty();
  }

  @Override
  public Pair<Entry, Row> next() {
    if (buffer.isEmpty()) {
      while (true) {
        BPlusTreeNode<Entry, Record> node = queue.poll();
        if (node instanceof BPlusTreeLeafNode) {
          int pageIndex = ((BPlusTreeLeafNode<Entry, Record>) node).getPageIndex();
          ArrayList<Row> pageRows = bufferManager.readPage(pageIndex);
          System.out.printf(
              "[Read page]: Page size %s, Node size %s, Index %s%n",
              pageRows.size(),
              node.nodeSize,
              ((BPlusTreeLeafNode<Entry, Record>) node).getPageIndex());
          try {
            for (int i = 0; i < node.nodeSize; i++) {
              buffer.add(new Pair<>(node.keys.get(i), pageRows.get(i)));
            }
          } catch (IndexOutOfBoundsException e) {
            System.out.println(
                "[INDEX OUT OF RANGE] "
                    + node.keys.size()
                    + " "
                    + node.nodeSize
                    + " "
                    + pageRows.size());
            e.printStackTrace();
            throw e;
          }
          break;
        } else if (node instanceof BPlusTreeInternalNode)
          for (int i = 0; i <= node.nodeSize; i++)
            queue.add(((BPlusTreeInternalNode<Entry, Record>) node).children.get(i));
      }
    }
    return buffer.poll();
  }
}
