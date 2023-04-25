package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
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
      if (!tableDir.exists())
        tableDir.mkdirs();

      String filePath = table.getTableMetaPath();
      try {
        FileOutputStream fileOutputStream = new FileOutputStream(filePath);
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
        for (Column column: table.getColumns()) {
          outputStreamWriter.write(column.toString() + "\n");
        }
        outputStreamWriter.close();
        fileOutputStream.close();
      } catch (Exception e) {
        System.out.println("database persist " + e);
        // TODO: add IO exception
      }
    }
  }

  public void create(String tableName, Column[] columns) {
    System.out.println(columns.length);

    Table newTable = new Table(this.name, tableName, columns);
    this.tables.put(tableName, newTable);
  }

  public void drop() {
    // TODO
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
      if (!file.isFile() || !file.getName().endsWith(Global.META_SUFFIX))
        continue;
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
        this.tables.put(tableName, newTable);
      } catch (Exception e) {

      }
    }
  }

  public void quit() {
    // TODO
  }

  public String getDatabaseDirPath(){
    return Global.DBMS_PATH + File.separator + "data" + File.separator + this.name;
  }
  public String getDatabaseTableFolderPath(){
    return this.getDatabaseDirPath() + File.separator + "tables";
  }
}
