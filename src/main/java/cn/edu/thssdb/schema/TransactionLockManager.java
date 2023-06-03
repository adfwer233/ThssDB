package cn.edu.thssdb.schema;

import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionLockManager {
  private ArrayList<ReentrantReadWriteLock> readLocks = new ArrayList<>();
  private ArrayList<ReentrantReadWriteLock> writeLocks = new ArrayList<>();

  public Table.TableHandler getTableHandler(
      Database database, String tableName, Boolean read, Boolean write) {
    Table.TableHandler tableHandler = database.getTable(tableName, read, write, this);
    if (read && Global.isolationLevel == Global.IsolationLevel.SERIALIZABLE) {
      readLocks.add(tableHandler.getTable().lock);
    }

    if (write) {
      writeLocks.add(tableHandler.getTable().lock);
    }
    return tableHandler;
  }

  public void releaseReadLock(ReentrantReadWriteLock lock) {
    if (readLocks.contains(lock)) {
      readLocks.remove(lock);
      lock.readLock().unlock();
    }
  }

  public void releaseLocks() {
    if (Global.isolationLevel == Global.IsolationLevel.SERIALIZABLE) {
      for (ReentrantReadWriteLock lock : readLocks) {
        try {
          System.out.println(
              String.format(
                  "[Read LOCK RELEASE before]"
                      + lock.getReadHoldCount()
                      + " "
                      + lock.getWriteHoldCount()));
          lock.readLock().unlock();
          System.out.println(
              String.format(
                  "[Read LOCK RELEASE after]"
                      + lock.getReadHoldCount()
                      + " "
                      + lock.getWriteHoldCount()));
        } catch (IllegalMonitorStateException e) {
          System.out.println(e.getMessage());
        }
      }
    }

    for (ReentrantReadWriteLock lock : writeLocks) {
      lock.writeLock().unlock();
      System.out.println(
          String.format(
              "[WRITE LOCK RELEASE]" + lock.getReadLockCount() + " " + lock.getWriteHoldCount()));
    }

    readLocks.clear();
    writeLocks.clear();
  }
}
