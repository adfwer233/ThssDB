package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.impl.PlanHandler;
import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private final HashMap<String, Database> databases;

  private final HashMap<Long, String> currentDatabaseName = new HashMap<>();
  public ArrayList<Long> currentSessions = new ArrayList<>();

  public Database.DatabaseHandler getCurrentDatabase(Long sessionId, Boolean read, Boolean write) {
    try {
      lock.readLock().lock();
      return getDatabaseHandler(currentDatabaseName.get(sessionId), read, write);
    } finally {
      lock.readLock().unlock();
    }
  }

  public Database.DatabaseHandler getDatabaseHandler(String dbName, Boolean read, Boolean write) {
    try {
      lock.readLock().lock();
      if (write) {
        return databases.get(dbName).getWriteHandler();
      } else if (read) {
        return databases.get(dbName).getReadHandler();
      }
    } finally {
      lock.readLock().unlock();
    }
    return null;
  }

  private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public void releaseTransactionLocks(Long sessionId) {
    for (Database database : databases.values()) {
      if (database.transactionLockManagers.containsKey(sessionId)) {
        database.transactionLockManagers.get(sessionId).releaseLocks();
      }
    }
  }

  public void persistCurrentDatabase(Long sessionId) {
    if (currentDatabaseName.containsKey(sessionId)) {
      databases.get(currentDatabaseName.get(sessionId)).persist(sessionId);
      if (Global.ENABLE_ROLLBACK) {
        databases.get(currentDatabaseName.get(sessionId)).undoLogger.clearLog();
      }
    }
  }

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public String showDb() {
    String res = "";
    for (Database db : databases.values()) {
      res += db.getName() + ",";
    }
    if (!res.isEmpty()) {
      res = res.substring(0, res.length() - 1);
    }
    return res;
  }

  public void persist(Long sessionId) {
    try {
      File managerFile = new File(Manager.getManagerDirPath());
      System.out.println(Manager.getManagerDataFilePath());
      System.out.println(managerFile.getAbsolutePath());
      if (!managerFile.exists()) managerFile.mkdirs();

      FileOutputStream fileOutputStream = new FileOutputStream(Manager.getManagerDataFilePath());
      OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
      for (Database database : this.databases.values()) {
        outputStreamWriter.write(database.getName() + "\n");
        database.persist(sessionId);
        database.logger.clearLog();
        database.undoLogger.clearLog();
      }
      outputStreamWriter.close();
      fileOutputStream.close();
    } catch (Exception e) {
      System.out.println("manager.persist " + e);
    }
  }

  public void recover() {
    File managerDataFile = new File(Manager.getManagerDataFilePath());
    if (!managerDataFile.isFile()) return;
    try {
      FileInputStream fileInputStream = new FileInputStream(managerDataFile);
      InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
      BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

      Long tmpSessionId = 2894759841L;
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println("recover database name: " + line);
        createDatabaseIfNotExists(line);
        Database database = databases.get(line);
        database.recover();
        database.transactionLockManagers.put(tmpSessionId, new TransactionLockManager(tmpSessionId));
        currentDatabaseName.put(tmpSessionId, database.getName());
        // recover from log
        ArrayList<String> logs = database.logger.readLog();
        for (String log : logs) {
          System.out.println("Recover: " + log);
          System.out.println(currentDatabaseName);
          LogicalPlan plan = LogicalGenerator.generate(log);
          // only single transaction logger-based recovery supported
          PlanHandler.handlePlan(plan, tmpSessionId, this);
        }
      }
      bufferedReader.close();
      inputStreamReader.close();
      fileInputStream.close();

      this.releaseTransactionLocks(tmpSessionId);

    } catch (Exception e) {

    }
  }

  public Manager() {
    databases = new HashMap<String, Database>();
    recover();
  }

  public void createDatabaseIfNotExists(String databaseName) {
    try {
      lock.writeLock().lock();
      if (databases.containsKey(databaseName)) throw new DatabaseExistException(databaseName);
      Database newDatabase = new Database(databaseName);
      databases.put(databaseName, newDatabase);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void deleteDatabase(String databaseName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(databaseName)) {
        throw new KeyNotExistException();
      }
      // delete the files
      File dbFolder = new File(databases.get(databaseName).getDatabaseTableFolderPath());
      File[] files = dbFolder.listFiles();
      for (File file : files) {
        System.out.println("[DROP DELETE] " + file.getPath());
        file.delete();
      }

      databases.remove(databaseName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void switchDatabase(Long sessionId, String databaseName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(databaseName)) {
        throw new KeyNotExistException();
      }
      System.out.println(sessionId);
      currentDatabaseName.put(sessionId, databaseName);

      Database db = databases.get(databaseName);
      if (!db.transactionLockManagers.containsKey(sessionId)) {
        System.out.println("init transaction manager");
        db.transactionLockManagers.put(sessionId, new TransactionLockManager(sessionId));
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void rollbackCurrentTransaction(Long sessionId) {
    // TODO: add session id for undo logger
    for (Database database : databases.values()) {
      database.rollback();
      database.undoLogger.clearLog();
      database.logger.clearLog();
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }

  public static String getManagerDirPath() {
    return Global.DBMS_PATH + File.separator + "data";
  }

  public static String getManagerDataFilePath() {
    return Global.DBMS_PATH + File.separator + "data" + File.separator + "manager";
  }
}
