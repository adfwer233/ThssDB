package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public HashMap<Long, TransactionLockManager> transactionLockManagers = new HashMap<>();

  public class DatabaseHandler implements AutoCloseable {
    private Database database;
    public Boolean hasReadLock;
    public Boolean hasWriteLock;

    public DatabaseHandler(Database database, Boolean read, Boolean write) {
      this.database = database;
      this.hasReadLock = read;
      this.hasWriteLock = write;

      if (read) {
        this.database.lock.readLock().lock();
      }
      if (write) {
        this.database.lock.writeLock().lock();
      }
    }

    public Database getDatabase() {
      return database;
    }

    @Override
    public void close() throws Exception {
      if (hasReadLock) {
        this.database.lock.readLock().unlock();
        hasReadLock = false;
      }
      if (hasWriteLock) {
        this.database.lock.writeLock().unlock();
        hasWriteLock = false;
      }
    }
  }

  public DatabaseHandler getReadHandler() {
    return new DatabaseHandler(this, true, false);
  }

  public DatabaseHandler getWriteHandler() {
    return new DatabaseHandler(this, false, true);
  }

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  public String getTableInfo(String tableName) throws TableNotExistException {
    if (!tables.containsKey(tableName)) {
      throw new TableNotExistException();
    }
    return tables.get(tableName).getTableInfo();
  }

  public String getName() {
    return name;
  }

  public void persist() {

    for (Table table : tables.values()) {
      String tableDirPath = table.getTableFolderPath();
      File tableDir = new File(tableDirPath);
      if (!tableDir.exists()) tableDir.mkdirs();

      // persist table meta
      String filePath = table.getTableMetaPath();
      try {
        FileOutputStream fileOutputStream = new FileOutputStream(filePath);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
        for (Column column : table.getColumns()) {
          outputStreamWriter.write(column.toString() + "\n");
        }
        outputStreamWriter.close();
        fileOutputStream.close();
      } catch (Exception e) {
        System.out.println("database persist " + e);
        // TODO: add IO exception
      }

      // serialize table data
      table.persist();
    }
  }

  public void create(String tableName, Column[] columns) {
    System.out.println(columns.length);

    Table newTable = new Table(this.name, tableName, columns);
    this.tables.put(tableName, newTable);
  }

  public void drop(String tableName) throws TableNotExistException {
    if (!tables.containsKey(tableName)) {
      throw new TableNotExistException();
    }
    this.tables.remove(tableName);
  }

  public void DeleteRow(Row row, String TableName) {
    this.tables.get(TableName).delete(row);
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  public void recover() {
    File tableFolder = new File(this.getDatabaseTableFolderPath());
    File[] files = tableFolder.listFiles();

    if (files == null) return;

    for (File file : files) {
      if (!file.isFile() || !file.getName().endsWith(Global.META_SUFFIX)) continue;
      String tableName = file.getName().replace(Global.META_SUFFIX, "");
      try {
        InputStreamReader inputStreamReader = new InputStreamReader(new FileInputStream(file));
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ArrayList<String> columnMetas = new ArrayList<String>();
        String tmpColumnMeta;
        while ((tmpColumnMeta = bufferedReader.readLine()) != null) {
          columnMetas.add(tmpColumnMeta);
        }
        inputStreamReader.close();
        bufferedReader.close();
        ArrayList<Column> columns = new ArrayList();
        for (String columnMeta : columnMetas) {
          columns.add(Column.createColumnFromMeta(columnMeta));
        }

        Table newTable = new Table(this.name, tableName, columns.stream().toArray(Column[]::new));

        // Recover the new table data from table file
        newTable.recover();

        this.tables.put(tableName, newTable);
      } catch (Exception e) {

      }
    }
  }

  public void quit() {
    // TODO
  }

  public String getDatabaseDirPath() {
    return Global.DBMS_PATH + File.separator + "data" + File.separator + this.name;
  }

  public String getDatabaseTableFolderPath() {
    return this.getDatabaseDirPath() + File.separator + "tables";
  }

  public Boolean isTableExist(String tableName) {
    return tables.containsKey(tableName);
  }

  public Table.TableHandler getTableForSession(
      Long sessionId, String tableName, Boolean read, Boolean write) {
    if (transactionLockManagers.containsKey(sessionId)) {
      return transactionLockManagers.get(sessionId).getTableHandler(this, tableName, read, write);
    } else {
      // TODO: Exception
      return getTable(tableName, read, write);
    }
  }

  Table.TableHandler getTable(String tableName, Boolean read, Boolean write) {

    if (read) {
      return tables.get(tableName).getReadHandler();
    } else if (write) {
      return tables.get(tableName).getWriteHandler();
    }
    return null;
  }
}
