package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.util.HashMap;
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

  private void persist() {
    // TODO
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

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }
}
