package cn.edu.thssdb.index;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Record;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.storage.BufferManager;
import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.Collections;

public class BPlusTreeLeafNode<K extends Comparable<K>, V extends Record>
    extends BPlusTreeNode<K, V> {

  ArrayList<V> values;
  private BPlusTreeLeafNode<K, V> next;
  private final PageCounter pageCounter;
  private final Integer pageIndex;
  private final BufferManager bufferManager;

  public Integer getPageIndex() {
    return pageIndex;
  }

  BPlusTreeLeafNode(int size, PageCounter pageCounter, BufferManager bufferManager) {
    keys = new ArrayList<>(Collections.nCopies((int) (1.5 * Global.fanout) + 1, null));
    values = new ArrayList<>(Collections.nCopies((int) (1.5 * Global.fanout) + 1, null));
    nodeSize = size;
    this.pageIndex = pageCounter.allocNewIndex();
    this.pageCounter = pageCounter;
    this.bufferManager = bufferManager;
  }

  private void valuesAdd(int index, V value) {
    for (int i = nodeSize; i > index; i--) values.set(i, values.get(i - 1));
    values.set(index, value);
  }

  private void valuesRemove(int index) {
    for (int i = index; i < nodeSize - 1; i++) values.set(i, values.get(i + 1));
  }

  @Override
  boolean containsKey(K key) {
    return binarySearch(key) >= 0;
  }

  @Override
  V get(K key) {
    int index = binarySearch(key);
    if (index >= 0) return values.get(index);
    throw new KeyNotExistException();
  }

  @Override
  BPlusTreeLeafNode<K, V> getLeafNode(K key) {
    int index = binarySearch(key);
    if (index >= 0) return this;
    //    System.out.println(this.keys.toString() + "---" +  key.toString());
    throw new KeyNotExistException();
  }

  @Override
  void put(K key, V value) {
    ArrayList<Row> page = bufferManager.readPage(pageIndex);
    int index = binarySearch(key);
    int valueIndex = index >= 0 ? index : -index - 1;
    System.out.printf("[B PLUS LEAF] SIZE %d %d index %d%n", nodeSize, page.size(), pageIndex);
    if (index >= 0) {
      System.out.println(keys.get(index));
      System.out.println(key);
      System.out.println(value);
      throw new DuplicateKeyException();
    } else {
      page.add(valueIndex, value.getContent());
      value.removeContent();
      valuesAdd(valueIndex, value);
      keysAdd(valueIndex, key);
    }
    bufferManager.writePage(pageIndex, page);
    //    nodeSize ++;
  }

  @Override
  void remove(K key) {
    ArrayList<Row> page = bufferManager.readPage(pageIndex);
    int index = binarySearch(key);
    if (index >= 0) {
      valuesRemove(index);
      keysRemove(index);
      page.remove(index);
    } else throw new KeyNotExistException();
    System.out.printf("[REMOVE %d %d]%n", page.size(), nodeSize);
    bufferManager.writePage(pageIndex, page);
  }

  public void update(K key, Integer columnIndex, Entry entry) {
    int index = binarySearch(key);
    if (index >= 0) {
      ArrayList<Row> page = bufferManager.readPage(pageIndex);
      Row row = page.get(index);
      row.getEntries().set(columnIndex, entry);
      page.set(index, row);
      bufferManager.writePage(pageIndex, page);
    } else throw new KeyNotExistException();
  }

  @Override
  K getFirstLeafKey() {
    return keys.get(0);
  }

  @Override
  BPlusTreeNode<K, V> split() {
    int from = (size() + 1) / 2;
    int to = size();
    BPlusTreeLeafNode<K, V> newSiblingNode =
        new BPlusTreeLeafNode<>(to - from, this.pageCounter, this.bufferManager);
    for (int i = 0; i < to - from; i++) {
      newSiblingNode.keys.set(i, keys.get(i + from));
      newSiblingNode.values.set(i, values.get(i + from));
      keys.set(i + from, null);
      values.set(i + from, null);
    }
    nodeSize = from;
    newSiblingNode.next = next;
    next = newSiblingNode;

    // split the pages
    ArrayList<Row> page = bufferManager.readPage(pageIndex);
    ArrayList<Row> siblingPage = new ArrayList<>();
    for (int i = from; i < to; i++) siblingPage.add(page.get(i));
    for (int i = from; i < to; i++) page.remove(page.size() - 1);
    System.out.printf(
        "[Split] %d %d %d %d%n",
        page.size(), siblingPage.size(), nodeSize, newSiblingNode.nodeSize);
    bufferManager.writePage(pageIndex, page);
    bufferManager.writePage(newSiblingNode.pageIndex, siblingPage);

    return newSiblingNode;
  }

  @Override
  void merge(BPlusTreeNode<K, V> sibling) {
    int index = size();
    BPlusTreeLeafNode<K, V> node = (BPlusTreeLeafNode<K, V>) sibling;
    int length = node.size();
    for (int i = 0; i < length; i++) {
      keys.set(i + index, node.keys.get(i));
      values.set(i + index, node.values.get(i));
    }
    nodeSize = index + length;
    next = node.next;

    // merge the pages
    ArrayList<Row> page = bufferManager.readPage(pageIndex);
    ArrayList<Row> siblingPage =
        bufferManager.readPage(((BPlusTreeLeafNode<K, Record>) sibling).pageIndex);
    page.addAll(siblingPage);
    bufferManager.writePage(pageIndex, page);
    pageCounter.removeIndex(((BPlusTreeLeafNode<K, Record>) sibling).pageIndex);
  }

  public BPlusTreeLeafNode<K, V> getNext() {
    return next;
  }
}
