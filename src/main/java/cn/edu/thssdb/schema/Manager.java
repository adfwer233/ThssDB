package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistException;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    databases = new HashMap<String, Database>();
    // TODO
  }

  public void createDatabaseIfNotExists(String databaseName) {
    // TODO
    if (databases.containsKey(databaseName))
      throw new DatabaseExistException(databaseName);
    // TODO: add throw
    Database newDatabase = new Database(databaseName);
    databases.put(databaseName, newDatabase);
  }

  private void deleteDatabase() {
    // TODO
  }

  public void switchDatabase() {
    // TODO
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
