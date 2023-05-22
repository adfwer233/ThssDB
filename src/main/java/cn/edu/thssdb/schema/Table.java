package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex = 0;
  public Table(String databaseName, String tableName, Column[] columns) {
    // TODO: add primary index
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].isPrimary()) {
        primaryIndex = i;
      }
    }

    if (primaryIndex == 0) {
      columns[0].setPrimary();
    }

    index = new BPlusTree<>();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList(Arrays.asList(columns));
  }

  public ArrayList<Column> getColumns() {
    return columns;
  }

  public String getTableInfo() {
    String res = String.format("Table name: %s \n", tableName);
    for (Column column : columns) {
      res = res.concat(String.format("%s \n", column.toString()));
    }
    return res;
  }

  private void recover() {
    // TODO
  }

  public void insert(ArrayList<Entry> entriesToInsert) {
    System.out.println("insert start");
    index.put(entriesToInsert.get(primaryIndex), new Row(entriesToInsert));
    System.out.println("insert success");
  }

  public String printTable() {
    String res = "";
    BPlusTreeIterator<Entry, Row> it = index.iterator();
    while (it.hasNext()) {
      Pair<Entry, Row> pair = it.next();
      res = res.concat(pair.right.toString() + ' ');
    }
    return res;
  }

  public void delete(Row row) {
    if (!this.containsRow(row)) {
      throw new KeyNotExistException();
    }
    this.index.remove(row.getEntries().get(this.primaryIndex));
  }

  public void update() {
    // TODO
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }

  public String getTableFolderPath() {
    return Global.DBMS_PATH
        + File.separator
        + "data"
        + File.separator
        + databaseName
        + File.separator
        + "tables";
  }

  private Boolean containsRow(Row row) {
    return this.index.contains(row.getEntries().get(this.primaryIndex));
  }

  public String getTablePath() {
    return getTableFolderPath() + File.separator + tableName;
  }

  public String getTableMetaPath() {
    return getTablePath() + Global.META_SUFFIX;
  }
}
