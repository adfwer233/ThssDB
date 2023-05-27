package cn.edu.thssdb.schema;

import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionLockManager {
  private ArrayList<ReentrantReadWriteLock> readLocks = new ArrayList<>();
  private ArrayList<ReentrantReadWriteLock> writeLocks = new ArrayList<>();

  public Table.TableHandler getTableHandler(
      Database database, String tableName, Boolean read, Boolean write) {
    Table.TableHandler tableHandler = database.getTable(tableName, read, write);
    if (read && Global.isolationLevel == Global.IsolationLevel.SERIALIZABLE) {
      readLocks.add(tableHandler.getTable().lock);
    }

    if (write) {
      writeLocks.add(tableHandler.getTable().lock);
    }
    return tableHandler;
  }

  public void releaseLocks() {
    if (Global.isolationLevel == Global.IsolationLevel.SERIALIZABLE) {
      for (ReentrantReadWriteLock lock : readLocks) {
        lock.readLock().unlock();
      }
    }

    for (ReentrantReadWriteLock lock : writeLocks) {
      lock.writeLock().unlock();
    }

    readLocks.clear();
    writeLocks.clear();
  }
}
