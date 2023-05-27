package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.io.File;
import java.nio.Buffer;
import java.util.ArrayList;

public class BufferManager {
  public final static Integer BufferSize = 10;

  private String tableName;
  private String tableDir;

  ArrayList<ArrayList<Row>> buffer = new ArrayList<>();

  ArrayList<Integer> bufferPageIndex = new ArrayList<>();

  ArrayList<Boolean> writeFlag = new ArrayList<>();
  public BufferManager(Table table) {
    tableName = table.tableName;
    tableDir = table.getTableFolderPath();
  }

  public void writePage(Integer PageIndex, ArrayList<Row> data) {

  }

  public void readPage(Integer pageIndex) {

  }

  public void flush() {

  }

  private String getPagePath(Integer page) {
    return tableDir + File.separator + tableName + page.toString();
  }
}
