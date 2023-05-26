package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex = 0;

  // table handler to manage the lock of table
  public class TableHandler implements AutoCloseable {

    private Table table;
    public Boolean hasReadLock;
    public Boolean hasWriteLock;

    public TableHandler(Table table, Boolean read, Boolean write) {
      this.table = table;
      this.hasReadLock = read;
      this.hasWriteLock = write;

      if (read) {
        table.lock.readLock().lock();
      }

      if (write) {
        table.lock.writeLock().lock();
      }
    }

    public Table getTable() {
      return table;
    }

    @Override
    public void close() {
      // only in read committed isolation level, S lock is released when close
      // in serializable isolation level, S lock is released when the transaction terminates(commit
      // or abort).
      if (Global.isolationLevel == Global.IsolationLevel.READ_COMMITTED) {
        if (this.hasReadLock) {
          this.table.lock.readLock().unlock();
          this.hasReadLock = false;
        }
      }
    }
  }

  public Table.TableHandler getReadHandler() {
    return new Table.TableHandler(this, true, false);
  }

  public Table.TableHandler getWriteHandler() {
    return new Table.TableHandler(this, false, true);
  }

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

  public void recover() {
    System.out.println("table recover " + getTablePath());
    ArrayList<Row> rows = deserialize();
    for (Row row : rows) {
      index.put(row.getEntries().get(primaryIndex), row);
    }
  }

  public void persist() {
    serialize();
  }

  public void insert(ArrayList<Entry> entriesToInsert) {
    System.out.println("insert start");
    index.put(entriesToInsert.get(primaryIndex), new Row(entriesToInsert));
    System.out.println("insert success");
  }

  public void insert(Row row) {
    index.put(row.getEntries().get(primaryIndex), row);
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
    try {
      File tableFolder = new File(getTableFolderPath());
      if (!tableFolder.exists()) tableFolder.mkdirs();

      File tableFile = new File(getTablePath());
      if (!tableFile.exists()) tableFile.createNewFile();

      // TODO: paging and multi-thread writing
      FileOutputStream fileOutputStream = new FileOutputStream(tableFile);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);

      for (Row row : this) {
        System.out.println("writing row " + getTablePath());
        objectOutputStream.writeObject(row);
      }

      objectOutputStream.close();
      fileOutputStream.close();
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  private ArrayList<Row> deserialize() {

    try {
      File tableFolder = new File(getTableFolderPath());
      if (!tableFolder.exists()) tableFolder.mkdirs();

      File tableFile = new File(getTablePath());
      if (!tableFile.exists()) return new ArrayList<>();

      // TODO: paging and multi-thread reading
      FileInputStream fileInputStream = new FileInputStream(tableFile);
      ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);

      ArrayList<Row> res = new ArrayList<>();
      System.out.println("read ready " + getTablePath());
      Object inputObject;
      while (fileInputStream.available() > 0) {
        System.out.println("read object " + getTablePath());
        inputObject = objectInputStream.readObject();
        res.add((Row) inputObject);
      }

      objectInputStream.close();
      fileInputStream.close();

      return res;
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }

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

  public Row parseRow(String rowString) {
    System.out.println("parse row " + rowString);
    String[] entryString = rowString.split(",");
    // TODO: Exception
    ArrayList<Entry> entries = new ArrayList<>();
    for (int i =0 ;i < columns.size(); i++) {
      switch (columns.get(i).getType()) {
        case STRING:
          entries.add(new Entry(entryString[i]));
          break;
        case INT:
          entries.add(new Entry(Integer.parseInt(entryString[i])));
          break;
        case LONG:
          entries.add(new Entry(Long.parseLong(entryString[i])));
          break;
        case DOUBLE:
          entries.add(new Entry(Double.parseDouble(entryString[i])));
          break;
        case FLOAT:
          entries.add(new Entry(Float.parseFloat(entryString[i])));
          break;
      }
    }
    return new Row(entries);
  }
}
