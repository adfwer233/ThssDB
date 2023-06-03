package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public HashMap<Long, TransactionLockManager> transactionLockManagers = new HashMap<>();
  public Logger logger;

  public Logger undoLogger;

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

    this.logger = new Logger(getDatabaseDirPath(), getDatabaseLogPath());
    this.undoLogger = new Logger(getDatabaseDirPath(), getDatabaseUndoPath());
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

    String databaseFolderPath = this.getDatabaseDirPath();
    File databaseFolder = new File(databaseFolderPath);

    if (!databaseFolder.exists()) databaseFolder.mkdirs();

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

  public void UpdateTable(Entry entry, Row row, String TableName) {
    this.tables.get(TableName).update(entry, row);
  }

  public void recover() {
    File tableFolder = new File(this.getDatabaseTableFolderPath());
    File[] files = tableFolder.listFiles();

    if (files == null) return;

    for (File file : files) {
      if (!file.isFile() || !file.getName().endsWith(Global.META_SUFFIX)) continue;
      String tableName = file.getName().replace(Global.META_SUFFIX, "");
      try {
        InputStreamReader inputStreamReader =
            new InputStreamReader(Files.newInputStream(file.toPath()));
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        ArrayList<String> columnMetas = new ArrayList<String>();
        String tmpColumnMeta;
        while ((tmpColumnMeta = bufferedReader.readLine()) != null) {
          columnMetas.add(tmpColumnMeta);
        }
        inputStreamReader.close();
        bufferedReader.close();
        ArrayList<Column> columns = new ArrayList<>();
        for (String columnMeta : columnMetas) {
          columns.add(Column.createColumnFromMeta(columnMeta));
        }

        Table newTable = new Table(this.name, tableName, columns.toArray(new Column[0]));
        System.out.println(columnMetas);
        // Recover the new table data from table file
        newTable.recover();

        this.tables.put(tableName, newTable);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void rollback() {
    ArrayList<String> undoLogs = undoLogger.readLog();
    System.out.println(undoLogs);
    for (int i = undoLogs.size() - 1; i >= 0; i--) {
      String[] res = undoLogs.get(i).split(" ");
      String tableName = res[1];
      if (res[0].equals("INSERT")) {
        // DELETE the row
        Row row = tables.get(tableName).parseRow(res[2]);
        System.out.println("Rollback " + res[2] + " " + row.toString());
        tables.get(tableName).delete(row);
      } else if (res[0].equals("DELETE")) {
        // INSERT the row
        Row row = tables.get(tableName).parseRow(res[2]);
        tables.get(tableName).insert(row);
      }
    }
  }

  public void quit() {
    try {
      this.lock.readLock().lock();
      this.persist();
      this.logger.clearLog();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public String getDatabaseDirPath() {
    return Global.DBMS_PATH + File.separator + "data" + File.separator + this.name;
  }

  public String getDatabaseTableFolderPath() {
    return this.getDatabaseDirPath() + File.separator + "tables";
  }

  public String getDatabaseLogPath() {
    return getDatabaseDirPath() + File.separator + "log";
  }

  public String getDatabaseUndoPath() {
    return getDatabaseDirPath() + File.separator + "undo";
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
