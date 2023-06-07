package cn.edu.thssdb.schema;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Logger {
  private String logPath;
  FileWriter fileWriter;

  public Logger(String dirPath, String path) {
    try {
      File logDir = new File(dirPath);
      if (!logDir.exists()) {
        logDir.mkdirs();
      }

      logPath = path;
      File logFile = new File(path);
      if (!logFile.exists()) {
        logFile.createNewFile();
      }
      fileWriter = new FileWriter(path, true);
    } catch (IOException ioException) {
      // TODO: throw exception and return to client
      System.out.println("Log init failed");
    }
  }

  public void writeLog(String statement) {
    try {
      //      System.out.println("Write log to" + logPath + " " + statement);
      fileWriter.write(statement + '\n');
      fileWriter.flush();
    } catch (IOException ioException) {
      // TODO: throw exception and return to client
      System.out.println("Log file failed");
    }
  }

  public void clearLog() {
    try {
      fileWriter.close();
      fileWriter = new FileWriter(logPath);
      fileWriter.write("");
      fileWriter.close();

      fileWriter = new FileWriter(logPath, true);
    } catch (IOException e) {
      System.out.println("clear Log file failed");
    }
  }

  public ArrayList<String> readLog() {
    ArrayList<String> logList = new ArrayList<>();
    try {
      InputStreamReader reader = new InputStreamReader(Files.newInputStream(Paths.get(logPath)));
      BufferedReader bufferedReader = new BufferedReader(reader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        logList.add(line);
      }
    } catch (Exception e) {
      // TODO: throw exception and return to client
      System.out.println("read log failed");
    }
    return logList;
  }
}
