package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeLeafNode;
import cn.edu.thssdb.index.PageCounter;
import cn.edu.thssdb.index.RecordTreeIterator;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.logging.Log;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  public ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  private final String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  private final BPlusTree<Entry, Record> index;
  public int primaryIndex = 0;

  public HashSet<Long> updateFlag = new HashSet<>();
  public Boolean updateMetaFlag = true;
  // table handler to manage the lock of table
  public class TableHandler implements AutoCloseable {

    private final Table table;
    public Boolean hasReadLock;
    public Boolean hasWriteLock;
    private Long s;

    public TableHandler(
        Table table, Boolean read, Boolean write, TransactionLockManager transactionLockManager) {
      this.table = table;
      this.hasReadLock = read;
      this.hasWriteLock = write;
      s = transactionLockManager.sessionId;

      if (read) {
        if (!table.lock.readLock().tryLock()) {
          table.lock.readLock().lock();
        }
//        while(!table.lock.readLock().tryLock());
      }

      if (write) {
        if (Global.isolationLevel == Global.IsolationLevel.READ_COMMITTED) {
          table.lock.writeLock().lock();
        } else if (Global.isolationLevel == Global.IsolationLevel.SERIALIZABLE) {
          if (table.lock.isWriteLocked()) {
            table.lock.writeLock().lock();
          } else {
            if (!table.lock.writeLock().tryLock()) {
              System.out.println("[UPDATE LOCK]");
              // 再次获取
              transactionLockManager.releaseReadLock(table.lock);
              table.lock.writeLock().lock();
            }
          }
        }
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

  public Table.TableHandler getReadHandler(TransactionLockManager transactionLockManager) {
    return new Table.TableHandler(this, true, false, transactionLockManager);
  }

  public Table.TableHandler getWriteHandler(TransactionLockManager transactionLockManager) {
    this.updateFlag.add(transactionLockManager.sessionId);
    return new Table.TableHandler(this, false, true, transactionLockManager);
  }

  public Table(String databaseName, String tableName, Column[] columns) {
    for (int i = 0; i < columns.length; i++) {
      if (columns[i].isPrimary()) {
        primaryIndex = i;
      }
    }

    if (primaryIndex == 0) {
      columns[0].setPrimary();
    }

    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));

    index = new BPlusTree<>(this);
  }

  public ArrayList<Column> getColumns() {
    return columns;
  }

  public int getPrimaryIndex() {
    return primaryIndex;
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

    try {
      File tableFolder = new File(getTableFolderPath());
      if (!tableFolder.exists()) tableFolder.mkdirs();

      File tableFile = new File(getTablePath());
      if (!tableFile.exists()) return;

      FileInputStream fileInputStream = new FileInputStream(tableFile);
      ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);

      PageCounter res;
      System.out.println("read ready " + getTablePath());
      Object inputObject;

      inputObject = objectInputStream.readObject();
      res = (PageCounter) inputObject;

      objectInputStream.close();
      fileInputStream.close();

      // recover the b+tree in memory
      for (Integer index : res.indexList) {
        ArrayList<Row> page = this.index.bufferManager.readPage(index);
        for (Row row : page) {
          Entry primary = row.getEntries().get(this.primaryIndex);
          this.index.put(primary, new Record());
        }
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  public void persist(Long sessionId) {
    if (!(updateFlag.contains(sessionId)))
      return;
    if (!this.lock.isWriteLockedByCurrentThread()) {
      return;
    }
    try {
      System.out.println("[Persist ]" + lock.isWriteLockedByCurrentThread() + " " + sessionId + " " + this.tableName);
      index.bufferManager.writeAllDirty();
      try {
        File tableFolder = new File(getTableFolderPath());
        if (!tableFolder.exists()) tableFolder.mkdirs();

        File tableFile = new File(getTableIndexPath());
        if (!tableFile.exists()) tableFile.createNewFile();

        System.out.println("[IO INDEX] " + tableFile);

        if (index.pageCounter.updated) {

          index.pageCounter.updated = false;

          FileOutputStream fileOutputStream = new FileOutputStream(tableFile);
          BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
          ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedOutputStream);

          objectOutputStream.writeObject(index.pageCounter);

          objectOutputStream.close();
          bufferedOutputStream.flush();
          bufferedOutputStream.close();
        }
      } catch (Exception e) {
        System.out.println(e.getMessage());
        e.printStackTrace();
      }
      updateFlag.remove(sessionId);
    } finally {
//      if (!lock.isWriteLockedByCurrentThread()) {
//        lock.readLock().unlock();
//      }
    }
  }

  public void insert(ArrayList<Entry> entriesToInsert, ArrayList<String> attrList) {
    ArrayList<Entry> entries = new ArrayList<>();

    if (attrList.isEmpty()) {
      entries.addAll(entriesToInsert);
    } else {
      for (Column column : columns) {
        int index = attrList.indexOf(column.getName());
        if (index >= 0) {
          entries.add(entriesToInsert.get(index));
        } else {
          entries.add(null);
        }
      }
    }

    Record record = new Record(new Row(entries));
    index.put(entries.get(primaryIndex), record);
  }

  public void removePrimaryKey(Entry key) {
    try {
      index.remove(key);
    }
    catch (KeyNotExistException e) {

    }
  }

  public void updateByPrimaryKey(Entry key, String columnName, Entry entry) {
    BPlusTreeLeafNode<Entry, Record> leafNode = index.getLeafNode(key);
    Integer columnIndex = Column2Index(columnName);
    try {
      leafNode.update(key, columnIndex, entry);
    } catch (KeyNotExistException e) {
      e.printStackTrace();
    }
  }

  public ArrayList<Row> getRowsByPrimaryKey(Entry key) {
    ArrayList<Row> rows = new ArrayList<>();
    try {
      BPlusTreeLeafNode<Entry, Record> leafNode = index.getLeafNode(key);
      while (leafNode != null) {

        Integer pageIndex = leafNode.getPageIndex();
        ArrayList<Row> page = index.bufferManager.readPage(pageIndex);
        boolean found = false;
        for (Row row : page) {
          if (row.getEntries().get(primaryIndex).equals(key)) {
            found = true;
            rows.add(row);
          }
        }
        if (!found) break;
        leafNode = leafNode.getNext();
      }
      //      System.out.println("[GET RESULT] " + rows.size());
      return rows;
    } catch (KeyNotExistException e) {
      //      e.printStackTrace();
      return rows;
    }
  }

  public void insert(Row row) {
    index.put(row.getEntries().get(primaryIndex), new Record(row));
  }

  public String printTable() {
    String res = "";
    for (Row row : this) {
      res = res.concat(row.toString() + '\n');
    }
    return res;
  }

  public void delete(Row row) {
    if (!this.containsRow(row)) {
      throw new KeyNotExistException();
    }
    this.index.remove(row.getEntries().get(this.primaryIndex));
  }

  public void update(Entry entry, Row newRow, ArrayList<String> attrList) {
    this.index.remove(entry);
    this.insert(newRow.getEntries(), attrList);
  }

  public int Column2Index(String columnName) {
    ArrayList<String> columnNames = new ArrayList<>();
    for (Column column : this.columns) {
      columnNames.add(column.getName());
    }
    return columnNames.indexOf(columnName);
  }

  private void serialize() {
    try {
      File tableFolder = new File(getTableFolderPath());
      if (!tableFolder.exists()) tableFolder.mkdirs();

      File tableFile = new File(getTablePath());
      if (!tableFile.exists()) tableFile.createNewFile();

      System.out.println("[IO] " + tableFile);
      // TODO: paging and multi-thread writing
      FileOutputStream fileOutputStream = new FileOutputStream(tableFile);
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedOutputStream);

      for (Row row : this) {
        objectOutputStream.writeObject(row);
      }

      objectOutputStream.close();
      bufferedOutputStream.flush();
      bufferedOutputStream.close();
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
    private final Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = new RecordTreeIterator(index, table);
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

  public String getTableIndexPath() {
    return getTablePath() + Global.INDEX_SUFFIX;
  }

  public Row parseRow(String rowString) {
    System.out.println("parse row " + rowString);
    String[] entryString = rowString.split(",");
    // TODO: Exception
    ArrayList<Entry> entries = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
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
