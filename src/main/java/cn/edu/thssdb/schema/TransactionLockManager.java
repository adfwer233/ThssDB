package cn.edu.thssdb.schema;

import cn.edu.thssdb.utils.Global;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionLockManager {
  private final ArrayList<ReentrantReadWriteLock> readLocks = new ArrayList<>();
  private final ArrayList<ReentrantReadWriteLock> writeLocks = new ArrayList<>();

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
          System.out.printf(
              "[Read LOCK RELEASE before]"
                  + lock.getReadHoldCount()
                  + " "
                  + lock.getWriteHoldCount()
                  + "%n");
          lock.readLock().unlock();
          System.out.printf(
              "[Read LOCK RELEASE after]"
                  + lock.getReadHoldCount()
                  + " "
                  + lock.getWriteHoldCount()
                  + "%n");
        } catch (IllegalMonitorStateException e) {
          System.out.println(e.getMessage());
        }
      }
    }

    for (ReentrantReadWriteLock lock : writeLocks) {
      lock.writeLock().unlock();
      System.out.printf(
          "[WRITE LOCK RELEASE]" + lock.getReadLockCount() + " " + lock.getWriteHoldCount() + "%n");
    }

    readLocks.clear();
    writeLocks.clear();
  }
}
