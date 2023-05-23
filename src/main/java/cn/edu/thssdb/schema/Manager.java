package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseExistException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.utils.Global;
import com.sun.org.apache.xpath.internal.operations.Bool;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;

  private HashMap<Long, String> currentDatabaseName = new HashMap<>();


  public Database getCurrentDatabase(Long sessionId) {
    return databases.get(currentDatabaseName.get(sessionId));
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

  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

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

  public void persist() {
    try {
      File managerFile = new File(Manager.getManagerDirPath());
      System.out.println(Manager.getManagerDataFilePath());
      System.out.println(managerFile.getAbsolutePath());
      if (!managerFile.exists()) managerFile.mkdirs();

      FileOutputStream fileOutputStream = new FileOutputStream(Manager.getManagerDataFilePath());
      OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
      for (Database database : this.databases.values()) {
        outputStreamWriter.write(database.getName() + "\n");
        System.out.println("persist " + database.getName());
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

      String line;
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println("recover database name: " + line);
        createDatabaseIfNotExists(line);
        Database database = databases.get(line);
        database.recover();
      }
      bufferedReader.close();
      inputStreamReader.close();
      fileInputStream.close();

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
    } finally {
      lock.writeLock().unlock();
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
