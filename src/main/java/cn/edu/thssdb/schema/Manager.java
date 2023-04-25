package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.TableNotExistException;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;

  private String currentDatabaseName;

  public Database getCurrentDatabase() {
    return databases.get(currentDatabaseName);
  }

  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public String showDb() {
    String res = "";
    for (Database db : databases.values ()) {
      res += db.getName () + ","; }
    if (!res.isEmpty ()) { res = res.substring (0, res.length () - 1); }
    return res;
  }

  public Manager() {
    databases = new HashMap<String, Database>();
    // TODO
  }

  public void createDatabaseIfNotExists(String databaseName) {
    if (databases.containsKey(databaseName)) throw new DatabaseExistException(databaseName);

    Database newDatabase = new Database(databaseName);
    databases.put(databaseName, newDatabase);
  }

  public void deleteDatabase(String databaseName) {
    if (!databases.containsKey(databaseName)) {
      throw new KeyNotExistException();
    }

    databases.remove(databaseName);
  }

  public void switchDatabase(String databaseName) {
    if (!databases.containsKey(databaseName)) {
      throw new KeyNotExistException();
    }

    currentDatabaseName = databaseName;
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
