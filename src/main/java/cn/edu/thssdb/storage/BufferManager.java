package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;

public class BufferManager {
  public static final Integer BufferSize = 100;

  private final String tableName;
  private final String tableDir;

  ArrayList<ArrayList<Row>> buffer = new ArrayList<>();
  ArrayList<Integer> bufferPageIndex = new ArrayList<>();

  ArrayList<Boolean> writeFlag = new ArrayList<>();

  public BufferManager(Table table) {
    tableName = table.tableName;
    tableDir = table.getTableFolderPath();
  }

  private void writeIO(Integer index, ArrayList<Row> page) {
    System.out.printf("[Page IO WRITE] [BUFFER SIZE %d] %s %d %d %n", buffer.size(), tableName, Runtime.getRuntime().maxMemory(), Runtime.getRuntime().totalMemory());
    System.out.flush();
    File folder = new File(tableDir);
    if (!folder.exists()) {
      folder.mkdirs();
    }

    File pagePath = new File(getPagePath(index));
    if (!pagePath.exists()) {
      try {
        pagePath.createNewFile();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    try {
      FileOutputStream fileOutputStream = new FileOutputStream(pagePath);
      BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(bufferedOutputStream);

      for (Row row : page) {
        objectOutputStream.writeObject(row);
      }

      objectOutputStream.close();
      bufferedOutputStream.flush();
      bufferedOutputStream.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void dropPage() {
    boolean found = false;
    for (int i = 0; i < buffer.size(); i++) {
      if (!writeFlag.get(i)) {
        writeIO(bufferPageIndex.get(i), buffer.get(i));
        buffer.remove(i);
        bufferPageIndex.remove(i);
        writeFlag.remove(i);
        found = true;
        break;
      }
    }

    if (!found) System.out.println("No page to drop");
    /*
     if all pages are dirty pages, no page will be dropped
     if a transaction write too many pages, the memory of buffer will be very large......
    */

  }

  private ArrayList<Row> getPage(Integer pageIndex) {
    File folder = new File(tableDir);
    if (!folder.exists()) {
      folder.mkdirs();
    }

    File pagePath = new File(getPagePath(pageIndex));
    if (!pagePath.exists()) {
      try {
        pagePath.createNewFile();
        return new ArrayList<>();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    ArrayList<Row> res = new ArrayList<>();
    try {
      System.out.printf("[Page IO Read] [BUFFER SIZE %d] %s%n", buffer.size(), pagePath);
      System.out.flush();
      FileInputStream fileInputStream = new FileInputStream(pagePath);

      if (fileInputStream.available() <= 0) {
        return res;
      }

      //      BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
      ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);

      Object inputObject;
      while (fileInputStream.available() > 0) {
        inputObject = objectInputStream.readObject();
        res.add((Row) inputObject);
      }

      fileInputStream.close();
      objectInputStream.close();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }

    return res;
  }

  public void writePage(Integer pageIndex, ArrayList<Row> data) {

    /*
     * If page in buffer
     * replace the current buffered page
     * set the dirty flag
     * */
    if (bufferPageIndex.contains(pageIndex)) {
      buffer.set(bufferPageIndex.indexOf(pageIndex), data);
      writeFlag.set(bufferPageIndex.indexOf(pageIndex), true);
      return;
    }

    /*
     * If buffer is not full
     * add the page to buffer
     * */
    buffer.add(data);
    bufferPageIndex.add(pageIndex);
    writeFlag.add(false);

    // buffer full
    if (buffer.size() > BufferSize) {
      dropPage();
    }
  }

  public ArrayList<Row> readPage(Integer pageIndex) {
    /*
     * If page in buffer, just return it
     * */
    if (bufferPageIndex.contains(pageIndex)) {
      //      System.out.println(String.format("[Page READ buffered] %s %s", pageIndex, tableName));
      return buffer.get(bufferPageIndex.indexOf(pageIndex));
    }

    // get page from file system
    ArrayList<Row> page = getPage(pageIndex);

    /*
     * read page and put it into buffer.
     * if the buffer is full, drop the first page.
     * */

    buffer.add(page);
    bufferPageIndex.add(pageIndex);
    writeFlag.add(false);

    if (buffer.size() > BufferSize) dropPage();

    return page;
  }

  public void flush() {
    while (!buffer.isEmpty()) {
      dropPage();
    }
  }

  public void writeAllDirty() {
    for (int i = 0; i < buffer.size(); i++) {
      if (writeFlag.get(i)) {
        writeIO(bufferPageIndex.get(i), buffer.get(i));
        writeFlag.set(i, false);
      }
    }
  }

  private String getPagePath(Integer page) {
    return tableDir + File.separator + tableName + Global.PAGE_SUFFIX + page.toString();
  }
}
